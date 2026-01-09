package sunyu.util;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.time.LocalDateTime;
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
        log.info("[构建参数] uri：{}", config.uri);
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

        public final String CREATE_TIME = "create_time";
        public final String UPDATE_TIME = "update_time";
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
     * @return 集合
     */
    public MongoCollection<Document> getCollection(MongoDatabase database, String collectionName) {
        return database.getCollection(collectionName);
    }

    /**
     * 将聚合管道转换为可以在MongoDB Compass的Aggregations中执行的JSON
     *
     * @param pipeline 聚合管道
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

    public InsertOneResult insertOne(MongoCollection<Document> collection, Map<String, ?> data) {
        Document document = new Document();
        document.append(config.CREATE_TIME, LocalDateTime.now());
        document.append(config.UPDATE_TIME, LocalDateTime.now());
        data.forEach(document::append);
        return collection.insertOne(document);
    }

    public InsertManyResult insertMany(MongoCollection<Document> collection, List<Map<String, ?>> dataList) {
        List<Document> documents = new ArrayList<>();
        for (Map<String, ?> data : dataList) {
            Document document = new Document();
            document.append(config.CREATE_TIME, LocalDateTime.now());
            document.append(config.UPDATE_TIME, LocalDateTime.now());
            data.forEach(document::append);
            documents.add(document);
        }
        return collection.insertMany(documents);
    }

    public UpdateResult saveOrUpdate(MongoCollection<Document> collection, Bson filter, Map<String, ?> data) {
        return saveOrUpdate(collection, filter, data, false);
    }

    public UpdateResult saveOrUpdate(MongoCollection<Document> collection, Bson filter, Map<String, ?> data, Boolean forceUpdate) {
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();

        // 构建更新操作
        List<Bson> updates = new ArrayList<>();

        // 设置create_time（只在插入时生效，使用$setOnInsert）
        updates.add(Updates.setOnInsert(config.CREATE_TIME, now));

        // 设置update_time（每次都会更新，使用$set）
        updates.add(Updates.set(config.UPDATE_TIME, now));

        // 设置数据字段（使用$set操作符）
        if (MapUtil.isNotEmpty(data)) {
            data.forEach((key, value) -> {
                if (Convert.toBool(forceUpdate, false)) {
                    // value是不是为空，都要更新到数据库
                    updates.add(Updates.set(key, value));
                } else if (value != null) {
                    // value不是空，才更新到数据库
                    if (value instanceof String) {
                        String nv = Convert.toStr(value, "").trim();
                        if (StrUtil.isNotBlank(nv)) {
                            // 不是空字符串，才更新到数据库
                            updates.add(Updates.set(key, nv));
                        }
                    } else {
                        updates.add(Updates.set(key, value));
                    }
                }
            });
        }

        // 执行upsert操作
        UpdateOptions options = new UpdateOptions().upsert(true);
        // 会更新所有匹配filter的数据
        return collection.updateMany(filter, Updates.combine(updates), options);
    }

    public DeleteResult delete(MongoCollection<Document> collection, Bson filter) {
        return collection.deleteMany(filter);
    }

    public BulkWriteResult bulkWrite(MongoCollection<Document> collection, List<? extends WriteModel<? extends Document>> requests) {
        return collection.bulkWrite(requests);
    }

}