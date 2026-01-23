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

        public MongoDBUtil build() {
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

    public InsertOneResult insertOneEntity(MongoCollection<Document> collection, Object entity) {
        Document document = toDocument(entity);
        document.append(config.CREATE_TIME, LocalDateTime.now());
        document.append(config.UPDATE_TIME, LocalDateTime.now());
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

    public InsertManyResult insertManyEntity(MongoCollection<Document> collection, List<Object> entityList) {
        List<Document> documents = new ArrayList<>();
        for (Object entity : entityList) {
            Document document = toDocument(entity);
            document.append(config.CREATE_TIME, LocalDateTime.now());
            document.append(config.UPDATE_TIME, LocalDateTime.now());
            documents.add(document);
        }
        return collection.insertMany(documents);
    }

    public UpdateResult saveOrUpdate(MongoCollection<Document> collection, Bson filter, Map<String, ?> data) {
        return saveOrUpdate(collection, filter, data, false);
    }

    public UpdateResult saveOrUpdateEntity(MongoCollection<Document> collection, Bson filter, Object entity) {
        return saveOrUpdateEntity(collection, filter, entity, false);
    }

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

    public DeleteResult delete(MongoCollection<Document> collection, Bson filter) {
        return collection.deleteMany(filter);
    }

    public BulkWriteResult bulkWrite(MongoCollection<Document> collection, List<? extends WriteModel<? extends Document>> requests) {
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
                return result.getInteger("__totalGroups");
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
        return findFirst(Document.class, mongoQuery);
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
        List<T> results = new ArrayList<>();
        try (MongoCursor<Document> mongoCursor = documents.iterator();) {
            while (mongoCursor.hasNext()) {
                results.add(toEntity(mongoCursor.next(), clazz));
            }
        }
        return results;
    }

    /**
     * 查询符合条件的文档列表
     *
     * @param mongoQuery 查询对象
     * @return MongoDB文档列表
     */
    public List<Document> find(MongoQuery mongoQuery) {
        return find(Document.class, mongoQuery);
    }

    /**
     * 分组查询
     *
     * @param mongoQuery 分组查询对象
     * @return 分组结果
     */
    public List<Document> group(MongoQuery mongoQuery) {
        List<Bson> pipeline = new ArrayList<>();

        // 设置过滤条件
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
        List<Document> results = new ArrayList<>();
        try (MongoCursor<Document> cursor = mongoQuery.getCollection().aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        return results;
    }


    public List<Document> leftOuterJoin(MongoQuery mongoQuery) {
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
        List<Document> results = new ArrayList<>();
        try (MongoCursor<Document> cursor = mongoQuery.getCollection().aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        return results;
    }

}