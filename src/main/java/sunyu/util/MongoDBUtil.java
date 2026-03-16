package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import sunyu.util.annotation.Column;
import sunyu.util.query.MongoQuery;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;

/**
 * MongoDB操作工具类
 * <p>
 * 该工具类封装了MongoDB的常用操作，提供简洁易用的API接口，
 * 包括文档的增删改查、聚合查询、分组统计、左外连接等功能。
 * </p>
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>文档操作：插入单条/多条、更新、删除</li>
 *   <li>查询操作：findFirst、find（支持列表返回和流式处理）</li>
 *   <li>聚合操作：分组统计、左外连接</li>
 *   <li>实体转换：支持Java对象与Document的相互转换</li>
 *   <li>自动时间戳：自动维护create_time和update_time字段</li>
 *   <li>时区支持：可配置默认时区，用于LocalDateTime与Date的转换</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>
 * // 创建工具类实例
 * MongoDBUtil mongoDBUtil = MongoDBUtil.builder()
 *     .setUri("mongodb://localhost:27017")
 *     .setDefaultZoneId(ZoneId.systemDefault())
 *     .build();
 *
 * try (mongoDBUtil) {
 *     // 获取数据库和集合
 *     MongoDatabase database = mongoDBUtil.getDatabase("test");
 *     MongoCollection&lt;Document&gt; collection = mongoDBUtil.getCollection(database, "users");
 *
 *     // 插入文档
 *     Map&lt;String, Object&gt; data = new HashMap&lt;&gt;();
 *     data.put("name", "张三");
 *     data.put("age", 25);
 *     mongoDBUtil.insertOne(collection, data);
 *
 *     // 查询文档
 *     MongoQuery query = new MongoQuery();
 *     query.setCollection(collection);
 *     query.setFilter(Filters.eq("name", "张三"));
 *     Document doc = mongoDBUtil.findFirst(query);
 * }
 * </pre>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>该类实现了AutoCloseable接口，建议使用try-with-resources语句自动释放资源</li>
 *   <li>实体转换需要使用{@link sunyu.util.annotation.Column @Column}注解标注字段</li>
 *   <li>时区配置默认为UTC，可通过Builder进行自定义</li>
 * </ul>
 *
 * @author sunyu
 * @version 1.0
 * @since 1.0
 * @see AutoCloseable
 * @see MongoQuery
 * @see sunyu.util.annotation.Column
 */
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

        /**
         * 默认时区配置
         * 默认使用UTC时区与MongoDB保持一致
         */
        public ZoneId defaultZoneId = ZoneOffset.UTC;
    }

    public static class Builder {
        private final Config config = new Config();

        /**
         * 构建MongoDBUtil实例
         *
         * @return MongoDBUtil实例
         * @throws IllegalArgumentException 如果uri未配置
         */
        public MongoDBUtil build() {
            // 参数校验
            if (StrUtil.isBlank(config.uri)) {
                throw new IllegalArgumentException("MongoDB URI cannot be blank");
            }
            return new MongoDBUtil(config);
        }

        // 可以针对私有属性进行初始化值方法
        public Builder setUri(String uri) {
            config.uri = uri;
            return this;
        }

        /**
         * 设置默认时区
         *
         * @param zoneId 时区ID
         */
        public Builder setDefaultZoneId(ZoneId zoneId) {
            config.defaultZoneId = zoneId;
            return this;
        }

        /**
         * 设置默认时区（基于TimeZone）
         *
         * @param timeZone 时区
         */
        public Builder setDefaultTimeZone(TimeZone timeZone) {
            config.defaultZoneId = timeZone.toZoneId();
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
     * 类型转换方法
     *
     * @param value      原始值
     * @param targetType 目标类型
     * @return 转换后的值
     */
    private Object convertType(Object value, Class<?> targetType) {
        // 处理null值
        if (value == null) {
            // 对于基本类型，返回默认值
            if (targetType == int.class) return 0;
            if (targetType == long.class) return 0L;
            if (targetType == double.class) return 0.0;
            if (targetType == boolean.class) return false;
            if (targetType == float.class) return 0.0f;
            if (targetType == short.class) return (short) 0;
            if (targetType == byte.class) return (byte) 0;
            if (targetType == char.class) return '\u0000';

            // 对于包装类型和其他引用类型，返回null
            return null;
        }

        // 如果类型已经匹配，直接返回
        if (targetType.isAssignableFrom(value.getClass())) {
            return value;
        }

        try {
            // 日期类型转换（使用配置的时区）
            if (targetType == LocalDateTime.class) {
                if (value instanceof Date) {
                    // 使用配置的时区进行转换
                    return ((Date) value).toInstant().atZone(config.defaultZoneId).toLocalDateTime();
                } else if (value instanceof String) {
                    return LocalDateTime.parse(value.toString());
                }
            } else if (targetType == Date.class) {
                if (value instanceof LocalDateTime) {
                    // 使用配置的时区进行转换
                    return Date.from(((LocalDateTime) value).atZone(config.defaultZoneId).toInstant());
                }
            }

            // MongoDB ObjectId 转换
            if (value instanceof ObjectId) {
                String strValue = value.toString();
                if (targetType == String.class) {
                    return strValue;
                }
            }

            // 基本类型转换
            if (targetType == String.class) {
                return value.toString();
            } else if (targetType == int.class || targetType == Integer.class) {
                return Integer.parseInt(value.toString());
            } else if (targetType == long.class || targetType == Long.class) {
                return Long.parseLong(value.toString());
            } else if (targetType == double.class || targetType == Double.class) {
                return Double.parseDouble(value.toString());
            } else if (targetType == boolean.class || targetType == Boolean.class) {
                return Boolean.parseBoolean(value.toString());
            } else if (targetType == float.class || targetType == Float.class) {
                return Float.parseFloat(value.toString());
            } else if (targetType == short.class || targetType == Short.class) {
                return Short.parseShort(value.toString());
            } else if (targetType == byte.class || targetType == Byte.class) {
                return Byte.parseByte(value.toString());
            }

        } catch (Exception e) {
            // 转换失败时，记录警告并返回null
            log.warn("类型转换失败: {} -> {}, 值: {} {}", value.getClass(), targetType, value, e.getMessage());
            return null;
        }

        return value;
    }

    /**
     * 获得数据库
     *
     * @param databaseName 数据库名称
     * @return 数据库
     */
    public MongoDatabase getDatabase(String databaseName) {
        // 参数校验
        if (StrUtil.isBlank(databaseName)) {
            throw new IllegalArgumentException("databaseName cannot be blank");
        }
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
        // 参数校验
        if (database == null) {
            throw new IllegalArgumentException("database cannot be null");
        }
        if (StrUtil.isBlank(collectionName)) {
            throw new IllegalArgumentException("collectionName cannot be blank");
        }
        return database.getCollection(collectionName);
    }

    /**
     * 将聚合管道转换为可以在MongoDB Compass的Aggregations中执行的JSON
     *
     * @param pipeline 聚合管道
     * @return Json字符串
     */
    public String toAggregationsJson(List<Bson> pipeline) {
        // 参数校验
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline cannot be null");
        }
        
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
     * 插入单条文档到指定集合
     * <p>
     * 该方法会自动为文档添加两个时间戳字段：
     * <ul>
     *   <li>create_time：创建时间，使用当前时间</li>
     *   <li>update_time：更新时间，使用当前时间</li>
     * </ul>
     * 时间戳使用配置的默认时区进行处理。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要插入文档的集合
     * @param data       要插入的文档数据，使用Map<String, ?>格式存储
     * @return InsertOneResult 插入操作的结果，包含插入文档的ID等信息
     * @throws IllegalArgumentException 如果 collection 或 data 为 null
     * @see MongoCollection
     * @see InsertOneResult
     * @see Map
     * @since 1.0
     * @author sunyu
     */
    public InsertOneResult insertOne(MongoCollection<Document> collection, Map<String, ?> data) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        
        Document document = new Document();
        document.append(config.CREATE_TIME, LocalDateTime.now());
        document.append(config.UPDATE_TIME, LocalDateTime.now());
        data.forEach(document::append);
        return collection.insertOne(document);
    }

    /**
     * 将实体对象转换为文档并插入到指定集合
     * <p>
     * 该方法会先使用 {@link #toDocument(Object)} 方法将实体对象转换为MongoDB Document，
     * 然后自动为文档添加两个时间戳字段：
     * <ul>
     *   <li>create_time：创建时间，使用当前时间</li>
     *   <li>update_time：更新时间，使用当前时间</li>
     * </ul>
     * 时间戳使用配置的默认时区进行处理。
     * </p>
     * <p>
     * <b>注意：</b>实体对象需要使用 {@link sunyu.util.annotation.Column @Column} 注解标注需要映射的字段。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要插入文档的集合
     * @param entity     要插入的实体对象
     * @return InsertOneResult 插入操作的结果，包含插入文档的ID等信息
     * @throws IllegalArgumentException 如果 collection 或 entity 为 null
     * @throws RuntimeException         如果实体转换为文档失败
     * @see MongoCollection
     * @see InsertOneResult
     * @see #toDocument(Object)
     * @see sunyu.util.annotation.Column
     * @since 1.0
     * @author sunyu
     */
    public InsertOneResult insertOneEntity(MongoCollection<Document> collection, Object entity) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (entity == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        
        Document document = toDocument(entity);
        document.append(config.CREATE_TIME, LocalDateTime.now());
        document.append(config.UPDATE_TIME, LocalDateTime.now());
        return collection.insertOne(document);
    }

    /**
     * 批量插入文档到指定集合
     * <p>
     * 该方法会为每个文档自动添加两个时间戳字段：
     * <ul>
     *   <li>create_time：创建时间，使用当前时间</li>
     *   <li>update_time：更新时间，使用当前时间</li>
     * </ul>
     * 时间戳使用配置的默认时区进行处理。
     * 所有文档会在一次批量操作中插入，提高性能。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要插入文档的集合
     * @param dataList   要插入的文档数据列表，每个元素使用Map<String, ?>格式存储
     * @return InsertManyResult 批量插入操作的结果，包含所有插入文档的ID等信息
     * @throws IllegalArgumentException 如果 collection 或 dataList 为 null
     * @throws IllegalArgumentException 如果 dataList 为空列表
     * @see MongoCollection
     * @see InsertManyResult
     * @see List
     * @since 1.0
     * @author sunyu
     */
    public InsertManyResult insertMany(MongoCollection<Document> collection, List<Map<String, ?>> dataList) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (dataList == null) {
            throw new IllegalArgumentException("dataList cannot be null");
        }
        if (dataList.isEmpty()) {
            throw new IllegalArgumentException("dataList cannot be empty");
        }
        
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

    /**
     * 批量将实体对象转换为文档并插入到指定集合
     * <p>
     * 该方法会先使用 {@link #toDocument(Object)} 方法将每个实体对象转换为MongoDB Document，
     * 然后为每个文档自动添加两个时间戳字段：
     * <ul>
     *   <li>create_time：创建时间，使用当前时间</li>
     *   <li>update_time：更新时间，使用当前时间</li>
     * </ul>
     * 时间戳使用配置的默认时区进行处理。
     * 所有文档会在一次批量操作中插入，提高性能。
     * </p>
     * <p>
     * <b>注意：</b>实体对象需要使用 {@link sunyu.util.annotation.Column @Column} 注解标注需要映射的字段。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要插入文档的集合
     * @param entityList 要插入的实体对象列表
     * @return InsertManyResult 批量插入操作的结果，包含所有插入文档的ID等信息
     * @throws IllegalArgumentException 如果 collection 或 entityList 为 null
     * @throws IllegalArgumentException 如果 entityList 为空列表
     * @throws RuntimeException         如果任一实体转换为文档失败
     * @see MongoCollection
     * @see InsertManyResult
     * @see List
     * @see #toDocument(Object)
     * @see sunyu.util.annotation.Column
     * @since 1.0
     * @author sunyu
     */
    public InsertManyResult insertManyEntity(MongoCollection<Document> collection, List<Object> entityList) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (entityList == null) {
            throw new IllegalArgumentException("entityList cannot be null");
        }
        if (entityList.isEmpty()) {
            throw new IllegalArgumentException("entityList cannot be empty");
        }
        
        List<Document> documents = new ArrayList<>();
        for (Object entity : entityList) {
            Document document = toDocument(entity);
            document.append(config.CREATE_TIME, LocalDateTime.now());
            document.append(config.UPDATE_TIME, LocalDateTime.now());
            documents.add(document);
        }
        return collection.insertMany(documents);
    }

    /**
     * 保存或更新文档（使用Map数据）
     * <p>
     * 该方法会根据过滤条件查找文档，如果找到则更新，否则插入新文档。
     * 默认不会更新空值，空字符串也会被忽略。
     * 会自动维护create_time和update_time字段。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要操作的集合
     * @param filter     过滤条件，用于查找要更新的文档
     * @param data       要保存或更新的数据，使用Map<String, ?>格式
     * @return UpdateResult 更新操作的结果，包含匹配和修改的文档数量等信息
     * @throws IllegalArgumentException 如果 collection 或 filter 为 null
     * @see MongoCollection
     * @see UpdateResult
     * @see #saveOrUpdate(MongoCollection, Bson, Map, Boolean)
     * @since 1.0
     * @author sunyu
     */
    public UpdateResult saveOrUpdate(MongoCollection<Document> collection, Bson filter, Map<String, ?> data) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (filter == null) {
            throw new IllegalArgumentException("filter cannot be null");
        }
        return saveOrUpdate(collection, filter, data, false);
    }

    /**
     * 保存或更新文档（使用实体对象）
     * <p>
     * 该方法会根据过滤条件查找文档，如果找到则更新，否则插入新文档。
     * 默认不会更新空值，空字符串也会被忽略。
     * 会自动维护create_time和update_time字段。
     * </p>
     * <p>
     * <b>注意：</b>实体对象需要使用 {@link sunyu.util.annotation.Column @Column} 注解标注需要映射的字段。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要操作的集合
     * @param filter     过滤条件，用于查找要更新的文档
     * @param entity     要保存或更新的实体对象
     * @return UpdateResult 更新操作的结果，包含匹配和修改的文档数量等信息
     * @throws IllegalArgumentException 如果 collection 或 filter 或 entity 为 null
     * @throws RuntimeException         如果实体转换为文档失败
     * @see MongoCollection
     * @see UpdateResult
     * @see #saveOrUpdateEntity(MongoCollection, Bson, Object, Boolean)
     * @see sunyu.util.annotation.Column
     * @since 1.0
     * @author sunyu
     */
    public UpdateResult saveOrUpdateEntity(MongoCollection<Document> collection, Bson filter, Object entity) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (filter == null) {
            throw new IllegalArgumentException("filter cannot be null");
        }
        if (entity == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        return saveOrUpdateEntity(collection, filter, entity, false);
    }

    /**
     * 保存或更新文档（使用实体对象，支持强制更新空值）
     * <p>
     * 该方法会根据过滤条件查找文档，如果找到则更新，否则插入新文档。
     * 会自动维护create_time和update_time字段。
     * </p>
     * <p>
     * <b>注意：</b>实体对象需要使用 {@link sunyu.util.annotation.Column @Column} 注解标注需要映射的字段。
     * </p>
     *
     * @param collection  MongoDB集合对象，指定要操作的集合
     * @param filter      过滤条件，用于查找要更新的文档
     * @param entity      要保存或更新的实体对象
     * @param forceUpdate 是否强制更新空值，true时即使值为空也会更新到数据库
     * @return UpdateResult 更新操作的结果，包含匹配和修改的文档数量等信息
     * @throws IllegalArgumentException 如果 collection 或 filter 或 entity 为 null
     * @throws RuntimeException         如果实体转换为文档失败
     * @see MongoCollection
     * @see UpdateResult
     * @see #toDocument(Object)
     * @see sunyu.util.annotation.Column
     * @since 1.0
     * @author sunyu
     */
    public UpdateResult saveOrUpdateEntity(MongoCollection<Document> collection, Bson filter, Object entity, Boolean forceUpdate) {
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();

        // 构建更新操作
        List<Bson> updates = new ArrayList<>();

        // 设置create_time（只在插入时生效，使用$setOnInsert）
        updates.add(Updates.setOnInsert(config.CREATE_TIME, now));

        // 设置update_time（每次都会更新，使用$set）
        updates.add(Updates.set(config.UPDATE_TIME, now));

        // 转换实体为文档
        Document document = toDocument(entity);

        // 设置数据字段（使用$set操作符）
        document.forEach((key, value) -> {
            if (key.equals(config.CREATE_TIME) || key.equals(config.UPDATE_TIME)) {
                // 这两个值在上面已经设置过了，这里不重复设置
                return;
            }
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

        // 执行upsert操作
        UpdateOptions options = new UpdateOptions().upsert(true);
        // 会更新所有匹配filter的数据
        return collection.updateMany(filter, Updates.combine(updates), options);
    }

    /**
     * 保存或更新文档（使用Map数据，支持强制更新空值）
     * <p>
     * 该方法会根据过滤条件查找文档，如果找到则更新，否则插入新文档。
     * 会自动维护create_time和update_time字段。
     * </p>
     *
     * @param collection  MongoDB集合对象，指定要操作的集合
     * @param filter      过滤条件，用于查找要更新的文档
     * @param data        要保存或更新的数据，使用Map<String, ?>格式
     * @param forceUpdate 是否强制更新空值，true时即使值为空也会更新到数据库
     * @return UpdateResult 更新操作的结果，包含匹配和修改的文档数量等信息
     * @throws IllegalArgumentException 如果 collection 或 filter 为 null
     * @see MongoCollection
     * @see UpdateResult
     * @since 1.0
     * @author sunyu
     */
    public UpdateResult saveOrUpdate(MongoCollection<Document> collection, Bson filter, Map<String, ?> data, Boolean forceUpdate) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (filter == null) {
            throw new IllegalArgumentException("filter cannot be null");
        }
        
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
                if (key.equals(config.CREATE_TIME) || key.equals(config.UPDATE_TIME)) {
                    // 这两个值在上面已经设置过了，这里不重复设置
                    return;
                }
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

    /**
     * 删除符合过滤条件的所有文档
     * <p>
     * 该方法会删除所有匹配过滤条件的文档。
     * 如果需要只删除单个文档，请使用 MongoDB 原生驱动的 deleteOne 方法。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要操作的集合
     * @param filter     过滤条件，用于查找要删除的文档
     * @return DeleteResult 删除操作的结果，包含删除的文档数量等信息
     * @throws IllegalArgumentException 如果 collection 或 filter 为 null
     * @see MongoCollection
     * @see DeleteResult
     * @since 1.0
     * @author sunyu
     */
    public DeleteResult delete(MongoCollection<Document> collection, Bson filter) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (filter == null) {
            throw new IllegalArgumentException("filter cannot be null");
        }
        return collection.deleteMany(filter);
    }

    /**
     * 执行批量写入操作
     * <p>
     * 该方法可以执行多个写入操作（插入、更新、删除）的组合，
     * 在一次批量操作中完成，提高性能。
     * </p>
     *
     * @param collection MongoDB集合对象，指定要操作的集合
     * @param requests   写入操作列表，可以包含插入、更新、删除等多种操作
     * @return BulkWriteResult 批量写入操作的结果，包含各操作的执行情况等信息
     * @throws IllegalArgumentException 如果 collection 或 requests 为 null
     * @throws IllegalArgumentException 如果 requests 为空列表
     * @see MongoCollection
     * @see BulkWriteResult
     * @see WriteModel
     * @since 1.0
     * @author sunyu
     */
    public BulkWriteResult bulkWrite(MongoCollection<Document> collection, List<? extends WriteModel<? extends Document>> requests) {
        // 参数校验
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (requests == null) {
            throw new IllegalArgumentException("requests cannot be null");
        }
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("requests cannot be empty");
        }
        return collection.bulkWrite(requests);
    }

    /**
     * 将Document转换为指定类型的实体对象（可配置是否跳过null值）
     *
     * @param document MongoDB文档
     * @param clazz    目标实体类
     * @param <T>      实体类型
     * @return 转换后的实体对象
     */
    public <T> T toEntity(Document document, Class<T> clazz) {
        if (document == null) {
            return null;
        }

        if (clazz == Document.class) {
            // 使用Class.cast()方法进行类型安全的转换，避免unchecked cast警告
            return clazz.cast(document);
        }

        try {
            T entity = clazz.getDeclaredConstructor().newInstance();

            // 获取所有字段
            Field[] fields = clazz.getDeclaredFields();

            for (Field field : fields) {
                // 获取@Column注解
                Column column = field.getAnnotation(Column.class);

                if (column != null) {
                    // 获取数据库列名
                    String dbColumnName = column.column();

                    // 从document中获取值
                    Object value = document.get(dbColumnName);

                    if (value == null) {
                        continue;
                    }

                    // 设置字段可访问
                    field.setAccessible(true);

                    // 类型转换并设置值（包括null值处理）
                    Object convertedValue = convertType(value, field.getType());

                    field.set(entity, convertedValue);
                }
            }

            return entity;
        } catch (Exception e) {
            throw new RuntimeException("文档转换失败: " + e.getMessage(), e);
        }
    }

    /**
     * 将实体对象转换为Document
     *
     * @param entity 实体对象
     * @return MongoDB文档
     */
    public Document toDocument(Object entity) {
        if (entity == null) {
            return null;
        }

        Document document = new Document();

        try {
            Class<?> clazz = entity.getClass();
            Field[] fields = clazz.getDeclaredFields();

            for (Field field : fields) {
                Column column = field.getAnnotation(Column.class);

                if (column != null) {
                    String dbColumnName = column.column();
                    field.setAccessible(true);
                    Object value = field.get(entity);

                    if (value != null) {
                        document.append(dbColumnName, value);
                    }
                }
            }

            return document;
        } catch (Exception e) {
            throw new RuntimeException("实体转换文档失败: " + e.getMessage(), e);
        }
    }

    /**
     * 统计符合查询条件的文档数量
     *
     * @param mongoQuery 查询对象
     * @return 文档数量
     */
    public long count(MongoQuery mongoQuery) {
        // 参数校验
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        if (mongoQuery.getCollection() == null) {
            throw new IllegalArgumentException("mongoQuery.getCollection() cannot be null");
        }
        
        if (CollUtil.isEmpty(mongoQuery.getGroupFields())) {
            return mongoQuery.getCollection().countDocuments(mongoQuery.getFilter());
        } else {
            List<Bson> pipeline = new ArrayList<>();

            // 设置过滤条件
            pipeline.add(Aggregates.match(mongoQuery.getFilter()));

            // 构建分组字段
            Document groupDocument = new Document();
            for (String groupField : mongoQuery.getGroupFields()) {
                groupDocument.append(groupField, "$" + groupField);
            }
            pipeline.add(Aggregates.group(groupDocument));
            pipeline.add(Aggregates.count("__totalGroups"));

            log.debug("Aggregation Pipeline {}: {}", mongoQuery.getCollection().getNamespace(), toAggregationsJson(pipeline));

            Document result = mongoQuery.getCollection().aggregate(pipeline).first();
            if (result != null) {
                return result.getLong("__totalGroups");
            }
        }
        return 0;
    }

    /**
     * 查询符合条件的第一个文档
     *
     * @param clazz      目标实体类
     * @param mongoQuery 查询对象
     * @param <T>        实体类型
     * @return 转换后的实体对象
     */
    public <T> T findFirst(Class<T> clazz, MongoQuery mongoQuery) {
        // 参数校验
        if (clazz == null) {
            throw new IllegalArgumentException("clazz cannot be null");
        }
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        if (mongoQuery.getCollection() == null) {
            throw new IllegalArgumentException("mongoQuery.getCollection() cannot be null");
        }
        
        FindIterable<Document> documents = mongoQuery.getCollection().find(mongoQuery.getFilter());
        if (mongoQuery.getProjection() != null) {
            documents.projection(mongoQuery.getProjection());
        }
        return toEntity(documents.first(), clazz);
    }

    /**
     * 查询符合条件的第一个文档
     *
     * @param mongoQuery 查询对象
     * @return MongoDB文档
     */
    public Document findFirst(MongoQuery mongoQuery) {
        // 参数校验
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        return findFirst(Document.class, mongoQuery);
    }


    /**
     * 查询符合条件的文档并通过消费者函数逐个处理
     * <p>
     * 该方法使用流式处理方式，避免一次性加载所有文档到内存中，
     * 适用于处理大量数据的场景。查询结果会以 Document 对象的形式
     * 逐个传递给消费者函数进行处理。
     * </p>
     *
     * @param mongoQuery 查询对象，包含查询条件、投影、分页等参数
     * @param consumer   消费者函数，用于处理每个查询到的 Document 对象
     * @throws IllegalArgumentException 如果 mongoQuery 或 consumer 为 null
     * @author sunyu
     * @see Consumer
     * @see Document
     * @see MongoQuery
     * @since 1.0
     */
    public void find(MongoQuery mongoQuery, Consumer<Document> consumer) {
        // 参数校验
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer cannot be null");
        }
        if (mongoQuery.getCollection() == null) {
            throw new IllegalArgumentException("mongoQuery.getCollection() cannot be null");
        }
        
        FindIterable<Document> documents = mongoQuery.getCollection().find(mongoQuery.getFilter());
        if (mongoQuery.getProjection() != null) {
            documents.projection(mongoQuery.getProjection());
        }
        if (mongoQuery.getSkip() != null) {
            documents.skip(mongoQuery.getSkip());
        }
        if (mongoQuery.getLimit() != null) {
            documents.limit(mongoQuery.getLimit());
        }
        try (MongoCursor<Document> mongoCursor = documents.iterator();) {
            while (mongoCursor.hasNext()) {
                consumer.accept(mongoCursor.next());
            }
        }
    }

    /**
     * 查询符合条件的文档列表
     *
     * @param clazz      目标实体类
     * @param mongoQuery 查询对象
     * @param <T>        实体类型
     * @return 转换后的实体对象列表
     */
    public <T> List<T> find(Class<T> clazz, MongoQuery mongoQuery) {
        // 参数校验
        if (clazz == null) {
            throw new IllegalArgumentException("clazz cannot be null");
        }
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        
        List<T> results = new ArrayList<>();
        find(mongoQuery, document -> results.add(toEntity(document, clazz)));
        return results;
    }

    /**
     * 查询符合条件的文档列表
     *
     * @param mongoQuery 查询对象
     * @return MongoDB文档列表
     */
    public List<Document> find(MongoQuery mongoQuery) {
        // 参数校验
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        return find(Document.class, mongoQuery);
    }

    /**
     * 执行分组查询并通过消费者函数逐个处理分组结果
     * <p>
     * 该方法使用MongoDB的聚合管道执行分组查询，支持：
     * <ul>
     *   <li>前置过滤条件</li>
     *   <li>多字段分组</li>
     *   <li>分组计数（可选）</li>
     *   <li>自定义投影</li>
     *   <li>排序</li>
     *   <li>分页</li>
     * </ul>
     * 结果会以Document对象的形式逐个传递给消费者函数进行处理，
     * 避免一次性加载所有分组结果到内存中，适用于处理大量数据的场景。
     * </p>
     *
     * @param mongoQuery 查询对象，包含以下关键参数：
     *                   <ul>
     *                     <li>filter：前置过滤条件</li>
     *                     <li>groupFields：分组字段列表</li>
     *                     <li>totalName：分组计数字段名（可选）</li>
     *                     <li>projection：自定义投影（可选）</li>
     *                     <li>sort：排序条件（可选）</li>
     *                     <li>skip：跳过记录数（可选）</li>
     *                     <li>limit：返回记录数（可选）</li>
     *                   </ul>
     * @param consumer   消费者函数，用于处理每个分组结果Document对象
     * @throws IllegalArgumentException 如果 mongoQuery 或 consumer 为 null
     * @throws IllegalArgumentException 如果 mongoQuery.getGroupFields() 为空
     * @author sunyu
     * @see Consumer
     * @see Document
     * @see MongoQuery
     * @see Aggregates
     * @since 1.0
     */
    public void group(MongoQuery mongoQuery, Consumer<Document> consumer) {
        List<Bson> pipeline = new ArrayList<>();

        // 设置前置过滤条件
        pipeline.add(Aggregates.match(mongoQuery.getFilter()));

        // 构建分组字段
        Document groupDocument = new Document();
        for (String groupField : mongoQuery.getGroupFields()) {
            groupDocument.append(groupField, "$" + groupField);
        }
        if (StrUtil.isNotBlank(mongoQuery.getTotalName())) {
            pipeline.add(Aggregates.group(groupDocument, Accumulators.sum(mongoQuery.getTotalName(), 1)));
        } else {
            pipeline.add(Aggregates.group(groupDocument));
        }

        // 构建投影字段
        if (mongoQuery.getProjection() != null) {
            pipeline.add(mongoQuery.getProjection());
        } else {
            List<Bson> projections = new ArrayList<>();
            projections.add(Projections.excludeId());

            // 遍历分组字段，生成computed投影
            for (String groupField : mongoQuery.getGroupFields()) {
                projections.add(Projections.computed(groupField, "$_id." + groupField));
            }

            // 如果总数字段不为空，则包含该字段
            if (StrUtil.isNotBlank(mongoQuery.getTotalName())) {
                projections.add(Projections.include(mongoQuery.getTotalName()));
            }

            pipeline.add(Aggregates.project(Projections.fields(projections)));
        }

        // 构建排序
        if (mongoQuery.getSort() != null) {
            pipeline.add(Aggregates.sort(mongoQuery.getSort()));
        }

        //设置分页参数
        if (mongoQuery.getSkip() != null) {
            pipeline.add(Aggregates.skip(mongoQuery.getSkip()));
        }
        if (mongoQuery.getLimit() != null) {
            pipeline.add(Aggregates.limit(mongoQuery.getLimit()));
        }

        log.debug("Aggregation Pipeline {}: {}", mongoQuery.getCollection().getNamespace(), toAggregationsJson(pipeline));

        // 查询
        try (MongoCursor<Document> cursor = mongoQuery.getCollection().aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                consumer.accept(cursor.next());
            }
        }
    }


    /**
     * 分组查询
     *
     * @param mongoQuery 分组查询对象
     * @return 分组结果
     */
    public List<Document> group(MongoQuery mongoQuery) {
        // 参数校验
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        
        List<Document> results = new ArrayList<>();
        group(mongoQuery, results::add);
        return results;
    }


    /**
     * 执行左外连接查询并通过消费者函数逐个处理连接结果
     * <p>
     * 该方法使用MongoDB的聚合管道执行左外连接查询，支持：
     * <ul>
     *   <li>主表前置过滤条件</li>
     *   <li>主表排序</li>
     *   <li>右表过滤条件</li>
     *   <li>右表字段投影</li>
     *   <li>限制右表返回记录数</li>
     *   <li>将右表字段合并到主文档（可选）</li>
     *   <li>主表字段投影</li>
     *   <li>分页</li>
     * </ul>
     * 结果会以Document对象的形式逐个传递给消费者函数进行处理，
     * 避免一次性加载所有连接结果到内存中，适用于处理大量数据的场景。
     * </p>
     *
     * @param mongoQuery 查询对象，包含以下关键参数：
     *                  <ul>
     *                    <li>filter：主表前置过滤条件</li>
     *                    <li>sort：主表排序条件（可选）</li>
     *                    <li>rightFilter：右表过滤条件</li>
     *                    <li>leftJoinField：主表连接字段</li>
     *                    <li>rightJoinField：右表连接字段</li>
     *                    <li>rightCollectionName：右表集合名称</li>
     *                    <li>rightProjection：右表字段投影（可选）</li>
     *                    <li>rightLimit：右表返回记录数限制（默认1）</li>
     *                    <li>mergeRightObjectsToLeft：是否将右表字段合并到主文档（可选）</li>
     *                    <li>projection：主表字段投影（可选）</li>
     *                    <li>skip：跳过记录数（可选）</li>
     *                    <li>limit：返回记录数（可选）</li>
     *                  </ul>
     * @param consumer   消费者函数，用于处理每个连接结果Document对象
     * @throws IllegalArgumentException 如果 mongoQuery 或 consumer 为 null
     * @throws IllegalArgumentException 如果必要的连接参数为空
     * @see Consumer
     * @see Document
     * @see MongoQuery
     * @see Aggregates
     * @since 1.0
     * @author sunyu
     */
    public void leftOuterJoin(MongoQuery mongoQuery, Consumer<Document> consumer) {
        List<Bson> pipeline = new ArrayList<>();

        // 设置过滤条件
        pipeline.add(Aggregates.match(mongoQuery.getFilter()));

        // 构建排序
        if (mongoQuery.getSort() != null) {
            pipeline.add(Aggregates.sort(mongoQuery.getSort()));
        }

        // 构建lookup的pipeline
        List<Bson> lookupPipeline = new ArrayList<>();
        lookupPipeline.add(
                // 在lookup内部进行过滤
                Aggregates.match(
                        Filters.and(
                                mongoQuery.getRightFilter(),
                                // 不带$符号的字段指的是被关联集合（右表）中的字段
                                // 带$符号的字段指的是主文档（左表）中的字段
                                Filters.expr(Filters.eq(mongoQuery.getRightJoinField(), "$" + mongoQuery.getLeftJoinField()))//使用expr引用外部字段
                        )
                )
        );
        if (mongoQuery.getRightProjection() != null) {
            // 排除不需要 右表 中的字段
            lookupPipeline.add(Aggregates.project(mongoQuery.getRightProjection()));
        }
        // 限制关联表的返回记录数量，默认只返回1条关联记录
        lookupPipeline.add(Aggregates.limit(mongoQuery.getRightLimit()));

        // 构建左外连接
        pipeline.add(
                Aggregates.lookup(
                        mongoQuery.getRightCollectionName(),//链接的外部稽核名称
                        lookupPipeline,
                        mongoQuery.getRightCollectionName() + "__datas"//链接结果储存在主文档中的字段名
                )
        );

        if (mongoQuery.getMergeRightObjectsToLeft() != null && mongoQuery.getMergeRightObjectsToLeft()) {
            pipeline.add(
                    // 使用$mergeObjects和$first替换根文档，将 右表 的字段合并到主文档
                    Aggregates.replaceWith(
                            // 创建$mergeObjects操作，用于合并多个文档
                            new Document(
                                    "$mergeObjects", // MongoDB聚合操作符：合并多个文档
                                    // 要合并的文档列表
                                    Arrays.asList(
                                            "$$ROOT", // 表示当前文档（主文档）
                                            // 创建$first操作，用于获取数组中的第一个元素
                                            new Document(
                                                    "$first", // MongoDB聚合操作符：获取数组第一个元素
                                                    "$" + mongoQuery.getRightCollectionName() + "__datas" // 从关联字段结果（数组）中获取第一个元素
                                            )
                                    )
                            )
                    )
            );
            pipeline.add(
                    Aggregates.project(Projections.fields(
                            Projections.exclude(mongoQuery.getRightCollectionName() + "__datas")// 已经进行合并了，那么这个列应该是没用了
                    ))
            );
        }

        // 排除 左表 不要的字段
        if (mongoQuery.getProjection() != null) {
            pipeline.add(Aggregates.project(mongoQuery.getProjection()));
        }

        //设置分页参数
        if (mongoQuery.getSkip() != null) {
            pipeline.add(Aggregates.skip(mongoQuery.getSkip()));
        }
        if (mongoQuery.getLimit() != null) {
            pipeline.add(Aggregates.limit(mongoQuery.getLimit()));
        }

        log.debug("Aggregation Pipeline {}: {}", mongoQuery.getCollection().getNamespace(), toAggregationsJson(pipeline));

        // 查询
        try (MongoCursor<Document> cursor = mongoQuery.getCollection().aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                consumer.accept(cursor.next());
            }
        }
    }

    public List<Document> leftOuterJoin(MongoQuery mongoQuery) {
        // 参数校验
        if (mongoQuery == null) {
            throw new IllegalArgumentException("mongoQuery cannot be null");
        }
        
        List<Document> results = new ArrayList<>();
        leftOuterJoin(mongoQuery, results::add);
        return results;
    }

}