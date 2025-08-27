package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDBUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private MongoDBUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());
        //其他初始化语句
        config.mongoClient = MongoClients.create(config.uri);
        log.info("[构建 {}] 结束", this.getClass().getSimpleName());
        this.config = config;
    }

    private static class Config {
        /**
         * mongodb客户端
         */
        private MongoClient mongoClient;
        /**
         * 链接mongodb的uri
         */
        private String uri;
    }

    public static class Builder {
        private final Config config = new Config();

        public MongoDBUtil build() {
            return new MongoDBUtil(config);
        }

        // 可以针对私有属性进行初始化值方法
        public Builder setUri(String uri) {
            config.uri = uri;
            return this;
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        // 回收各种资源
        log.info("[销毁 {}] 开始", this.getClass().getSimpleName());
        config.uri = null;
        config.mongoClient.close();
        log.info("[销毁 {}] 结束", this.getClass().getSimpleName());
    }

    /**
     * 获得数据库
     *
     * @param databaseName 数据库名称
     *
     * @return 数据库
     */
    public MongoDatabase getDatabase(String databaseName) {
        return config.mongoClient.getDatabase(databaseName);
    }

    /**
     * 获得集合
     *
     * @param database       数据库
     * @param collectionName 集合名称
     *
     * @return 集合
     */
    public MongoCollection<Document> getCollection(MongoDatabase database, String collectionName) {
        return database.getCollection(collectionName);
    }

    /**
     * 将聚合管道转换为可以在MongoDB Compass的Aggregations中执行的JSON
     *
     * @param pipeline 聚合管道
     *
     * @return Json字符串
     */
    public String toAggregationsJson(List<Bson> pipeline) {
        List<String> stageJsons = new ArrayList<>();
        // 关键点：指定 Shell 模式的序列化配置
        JsonWriterSettings settings = JsonWriterSettings.builder()
                .outputMode(JsonMode.SHELL)
                .build();
        for (Bson stage : pipeline) {
            String stageJson = stage.toBsonDocument().toJson(settings); // 传入配置
            stageJsons.add(stageJson);
        }
        return "[" + String.join(",", stageJsons) + "]";
    }


    /**
     * 将Map结构转换为MongoDB Document对象
     *
     * <p>此方法用于将Java Map中的键值对逐个复制到MongoDB Document中，
     * 主要用于构建可持久化到MongoDB集合的文档对象</p>
     *
     * @param map 包含要转换的数据源Map，键值对类型为String到任意类型
     *
     * @return 转换后的MongoDB Document对象
     *
     * <p>注意事项：</p>
     * <ul>
     *     <li>空值处理：允许value为null值，MongoDB会存储为null类型</li>
     *     <li>类型兼容：value需为MongoDB支持的数据类型（如基本类型、Date、嵌套Map等）</li>
     *     <li>键冲突：若map中包含重复键，后续值会覆盖前面的值</li>
     * </ul>
     * <p>
     * 示例用法：
     * <pre>
     * Map<String, Object> data = new HashMap<>();
     * data.put("name", "Alice");
     * data.put("age", 30);
     * Document doc = mongoDBUtil.mapToDocument(data);
     * collection.insertOne(doc);
     * </pre>
     */
    public Document mapToDocument(Map<String, ?> map) {
        Document doc = new Document();
        map.forEach(doc::append);
        return doc;
    }


}