package sunyu.util.test;

import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sunyu.util.MongoDBUtil;
import sunyu.util.query.MongoQuery;
import sunyu.util.test.entity.SimInfo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MongoDB工具类测试类
 * 用于测试MongoDBUtil工具类的各种功能，包括连接、查询、聚合等操作
 */
public class MongoDBUtilTests {
    // 日志记录器
    static Log log = LogFactory.get();

    static Props props = ConfigProperties.getProps();

    JSONConfig jsonConfig = JSONConfig.create().setDateFormat("yyyy-MM-dd HH:mm:ss");

    // MongoDB工具类实例
    static MongoDBUtil mongoDBUtil;
    // SIM数据库实例
    static MongoDatabase simDatabase;
    // SIM信息集合实例
    static MongoCollection<Document> simInfoCollection;
    static MongoCollection<Document> trafficInfoCollection;
    static MongoCollection<Document> testCollection;

    static List<String> dateFormats = Arrays.asList(
            "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SS", "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss"
            , "yyyy/MM/dd HH:mm:ss", "yyyy/M/d HH:mm:ss", "yyyy/MM/dd", "yyyy/M/d"
            , "yyyy-M-d HH:mm:ss"
            , "yyyy-MM-dd", "yyyy-M-d"
            , "yyyy年M月d日", "yyyy年MM月dd日"
    );

    /**
     * 在所有测试方法执行前运行
     * 初始化MongoDB连接和相关工具类
     */
    @BeforeAll
    static void beforeClass() {
        // 创建MongoDB工具类实例并设置连接URI
        mongoDBUtil = MongoDBUtil.builder()
                // MongoDB连接字符串，包含用户名、密码、主机地址和连接参数
                .setUri(props.getStr("mongodb.uri"))
                .build();
        // 获取sim数据库实例
        simDatabase = mongoDBUtil.getDatabase("sim");
        // 获取sim_info集合实例
        simInfoCollection = mongoDBUtil.getCollection(simDatabase, "sim_info");
        // 获取traffic_info集合实例
        trafficInfoCollection = mongoDBUtil.getCollection(simDatabase, "traffic_info");
        // 用于测试的集合
        testCollection = mongoDBUtil.getCollection(simDatabase, "test");
    }

    /**
     * 在所有测试方法执行后运行
     * 关闭MongoDB连接，释放资源
     */
    @AfterAll
    static void afterClass() {
        mongoDBUtil.close();
    }

    @Test
    void 统计全部sim卡数量() {
        log.info("sim_info集合中的文档数量: {}", mongoDBUtil.count(new MongoQuery(simInfoCollection)));
    }

    @Test
    void 统计未来一个月服务到期的卡数量() {
        Bson filter = Filters.lt("service_expiration_time", LocalDate.now().plusMonths(1));
        log.info("未来一个月服务到期的卡数量: {}", mongoDBUtil.count(new MongoQuery(simInfoCollection).setFilter(filter)));
    }

    @Test
    void 统计无卡号信息卡数量() {
        // 查询不存在sim_msisdn字段、或者字段为null、或者字段为空字符串的文档
        Bson filter = Filters.or(
                // 字段不存在
                Filters.exists("sim_msisdn", false),
                // 字段值为null
                Filters.eq("sim_msisdn", null),
                // 字段为空字符串或仅包含空白字符
                Filters.regex("sim_msisdn", "^\\s*$")
        );
        log.info("无卡号信息的卡数量: {}", mongoDBUtil.count(new MongoQuery(simInfoCollection).setFilter(filter)));
    }

    @Test
    void 统计没有卡号字段的卡数量() {
        log.info("没有卡号字段的卡数量: {}", mongoDBUtil.count(new MongoQuery(simInfoCollection).setFilter(Filters.exists("sim_msisdn", false))));
    }

    @Test
    void 统计sim_msisdn为null的卡数量() {
        Bson filter = Filters.eq("sim_msisdn", null);
        log.info("sim_msisdn为null的卡数量: {}", mongoDBUtil.count(new MongoQuery(simInfoCollection).setFilter(filter)));

        Document document = mongoDBUtil.findFirst(new MongoQuery(simInfoCollection).setFilter(filter));
        log.info("sim_msisdn为null的卡示例: {}", document.toJson());
    }

    @Test
    void 读取一个文档() {
        log.info("读取一个文档");
        Document document = mongoDBUtil.findFirst(new MongoQuery(simInfoCollection));
        if (document != null) {
            log.info("原始文档: {}", document.toJson());
            try {
                // 使用基于注解的转换器将文档转换成 SimInfo 对象（跳过null值，避免类型转换错误）
                SimInfo simInfo = mongoDBUtil.toEntity(document, SimInfo.class);
                log.info("转换后的SimInfo对象: {}", simInfo);
                log.info("转换后的SimInfo对象JSON: {}", JSONUtil.toJsonStr(simInfo, jsonConfig));
                log.info("ICCID: {}", simInfo.getSimIccid());
                log.info("手机号: {}", simInfo.getSimMsisdn());
                log.info("设备ID: {}", simInfo.getDeviceId());
                log.info("创建时间: {}", simInfo.getCreateTime());
                log.info("更新时间: {}", simInfo.getUpdateTime());
            } catch (Exception e) {
                log.error("转换失败: {}", e.getMessage(), e);
            }
        } else {
            log.info("没有找到文档");
        }
    }

    @Test
    void 读取一个文档2() {
        log.info("读取一个文档");
        SimInfo simInfo = mongoDBUtil.findFirst(SimInfo.class, new MongoQuery(simInfoCollection));
        if (simInfo != null) {
            log.info("转换后的SimInfo对象: {}", simInfo);
            log.info("转换后的SimInfo对象JSON: {}", JSONUtil.toJsonStr(simInfo, jsonConfig));
            log.info("ICCID: {}", simInfo.getSimIccid());
            log.info("手机号: {}", simInfo.getSimMsisdn());
            log.info("设备ID: {}", simInfo.getDeviceId());
            log.info("创建时间: {}", simInfo.getCreateTime());
            log.info("更新时间: {}", simInfo.getUpdateTime());
        } else {
            log.info("没有找到文档");
        }
    }

    @Test
    void 分页查询() {
        int page = 1;
        int pageSize = 10;
        List<SimInfo> simInfos = mongoDBUtil.find(SimInfo.class, new MongoQuery(simInfoCollection).setPage(page, pageSize));
        for (SimInfo simInfo : simInfos) {
            log.info("{}", JSONUtil.toJsonStr(simInfo, jsonConfig));
        }
    }

    @Test
    void 多条件查询() {
        int page = 1;
        int pageSize = 10;
        Bson projection = Projections.excludeId();
        Bson sort = Sorts.orderBy(Sorts.descending("update_time"));
        Bson filter = Filters.empty();

        List<SimInfo> simInfos = mongoDBUtil.find(SimInfo.class,
                new MongoQuery(simInfoCollection)
                        .setFilter(filter).setProjection(projection).setSort(sort).setPage(page, pageSize));
        for (SimInfo simInfo : simInfos) {
            log.info("{}", JSONUtil.toJsonStr(simInfo, jsonConfig));
        }

        long total = mongoDBUtil.count(new MongoQuery(simInfoCollection).setFilter(filter));
        log.info("{}", total);
    }

    @Test
    void 限制返回列() {
        SimInfo simInfo = mongoDBUtil.findFirst(SimInfo.class, new MongoQuery(simInfoCollection).setProjection(
                Projections.fields(Projections.include("sim_msisdn", "sim_status")))
        );
        log.info("{}", JSONUtil.toJsonStr(simInfo, jsonConfig));
    }

    /**
     * 分组SIM卡状态和数量
     */
    @Test
    void 分组SIM卡状态和数量() {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.group("$sim_status", Accumulators.sum("count", 1)),
                // 添加投影阶段，将_id重命名为status
                Aggregates.project(
                        Projections.fields(
                                Projections.excludeId(),
                                Projections.computed("status", "$_id"),
                                Projections.include("count")
                        )
                ),
                Aggregates.sort(Sorts.descending("count"))
        );

        // 执行聚合查询
        try (MongoCursor<Document> cursor = simInfoCollection.aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                log.info("状态: {}, 数量: {}", doc.get("status"), doc.get("count"));
            }
        }
    }

    @Test
    void 分组SIM卡状态和数量2() {
        MongoQuery mongoQuery = new MongoQuery(simInfoCollection)
                .setGroupField("sim_status").setTotalName("count")
                .setSort(Sorts.descending("count"))
                .setPage(1, 5);
        List<Document> results = mongoDBUtil.group(mongoQuery);
        for (Document result : results) {
            log.info("{}", result.toJson());
        }
        long total = mongoDBUtil.count(mongoQuery);
        log.info("{}", total);
    }

    @Test
    void 测试时间() {
        LocalDateTime t = LocalDateTimeUtil.parse("2025-11-02T12:13:14");
        // 获取月份第一天的0点0分0秒
        LocalDateTime a = t.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        // 获取月份最后一天的23点59分59秒999毫秒
        int lastDayOfMonth = t.getMonth().length(t.toLocalDate().isLeapYear());
        LocalDateTime b = t.withDayOfMonth(lastDayOfMonth)
                .withHour(23).withMinute(59).withSecond(59).withNano(999999999);
        log.info("{}", a);
        log.info("{}", b);
    }

    @Test
    void traffic_info链接sim_info2() {
        LocalDateTime a = LocalDateTimeUtil.parse("2025-12-01T00:00:00");
        LocalDateTime b = LocalDateTimeUtil.parse("2026-01-01T00:00:00");
        List<Document> documents = mongoDBUtil.leftOuterJoin(new MongoQuery(trafficInfoCollection)
                // 左表过滤
                .setFilter(Filters.and(
                        Filters.gte("traffic_time", a),
                        Filters.lt("traffic_time", b)
                ))
                // 左表排序
                .setSort(Sorts.descending("billed_volume"))
                // 左外连接
                .setRightCollectionName("sim_info")
                // 关联字段
                .setLeftJoinField("sim_iccid").setRightJoinField("sim_iccid")
                // 右表字段过滤
                .setRightProjection(Projections.fields(
                        Projections.excludeId(),
                        Projections.exclude("create_time", "update_time")
                ))
                // 是否将右表第一条内容合并到左表中
                .setMergeRightObjectsToLeft(true)
                // 左表翻页
                .setPage(1, 5)
                // 返回左表字段
                .setProjection(Projections.excludeId())
        );
        for (Document document : documents) {
            log.info("{}", JSONUtil.toJsonStr(document, jsonConfig));
        }
    }

    @Test
    void traffic_info链接sim_info() {
        LocalDateTime a = LocalDateTimeUtil.parse("2025-12-01T00:00:00");
        LocalDateTime b = LocalDateTimeUtil.parse("2026-01-01T00:00:00");
//        LocalDateTime a = LocalDateTimeUtil.parse("2020-12-01T00:00:00");
//        LocalDateTime b = LocalDateTimeUtil.parse("2021-01-01T00:00:00");
        List<Bson> pipeline = Arrays.asList(
                // 匹配时间范围条件
                Aggregates.match(Filters.and(
                        Filters.gte("traffic_time", a),
                        Filters.lt("traffic_time", b)
                )),
                // 按billed_volume字段降序排序
                Aggregates.sort(Sorts.descending("billed_volume")),
                // 使用带有pipeline的lookup连接两个集合
                Aggregates.lookup(
                        "sim_info", // 连接的外部集合名称
                        Arrays.asList(
                                // 在lookup内部进行过滤
                                Aggregates.match(
                                        Filters.and(
                                                // 不带$符号的字段指的是被关联集合（右表）中的字段
                                                // 带$符号的字段指的是主文档（左表）中的字段
                                                Filters.expr(Filters.eq("sim_iccid", "$sim_iccid"))// 使用$expr引用主文档的sim_iccid字段
                                        )
                                ),
                                // 投影，排除不需要的字段
                                Aggregates.project(Projections.fields(
                                        Projections.excludeId(),
                                        Projections.exclude("create_time", "update_time")
                                )),
                                // 限制每个sim_iccid只返回一条记录
                                Aggregates.limit(1)
                        ),
                        "sim_info_datas" // 连接结果存储在主文档中的字段名
                ),
                // 使用$mergeObjects和$first替换根文档，将sim_info的字段合并到主文档
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
                                                "$sim_info_datas" // 从sim_info_datas字段（数组）中获取第一个元素
                                        )
                                )
                        )
                ),
                // 投影，排除不需要的字段
                Aggregates.project(Projections.fields(
                        Projections.excludeId(),
                        Projections.exclude("sim_info_datas")
                )),
                // 跳过前0条记录
                Aggregates.skip(0),
                // 限制返回结果数量为10个
                Aggregates.limit(10)
        );
        log.info("{}", mongoDBUtil.toAggregationsJson(pipeline));
        try (MongoCursor<Document> cursor = trafficInfoCollection.aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                log.info("{}", doc.toJson());
            }
        }
    }

    @Test
    void sim_info链接traffic_info2() {
        LocalDateTime a = LocalDateTimeUtil.parse("2025-12-01T00:00:00");
        LocalDateTime b = LocalDateTimeUtil.parse("2026-01-01T00:00:00");
        List<Document> documents = mongoDBUtil.leftOuterJoin(new MongoQuery(simInfoCollection)
                .setSort(Sorts.descending("update_time"))
                .setRightCollectionName("traffic_info")
                .setLeftJoinField("sim_iccid").setRightJoinField("sim_iccid")
                /*.setRightFilter(
                        Filters.and(
                                Filters.gte("traffic_time", a),
                                Filters.lt("traffic_time", b)
                        )
                )*/
                .setRightLimit(10)
                .setRightProjection(Projections.fields(
                        Projections.excludeId(),
                        Projections.exclude("create_time", "update_time")
                ))
                .setPage(1, 5)
                .setProjection(Projections.fields(
                        Projections.excludeId()
                ))
        );
        for (Document document : documents) {
            log.info("{}", JSONUtil.toJsonStr(document, jsonConfig));
        }
    }

    @Test
    void sim_info链接traffic_info() {
        LocalDateTime a = LocalDateTimeUtil.parse("2025-12-01T00:00:00");
        LocalDateTime b = LocalDateTimeUtil.parse("2026-01-01T00:00:00");
//        LocalDateTime a = LocalDateTimeUtil.parse("2020-12-01T00:00:00");
//        LocalDateTime b = LocalDateTimeUtil.parse("2021-01-01T00:00:00");

        // 使用sim_info中的sim_iccid链接traffic_info中的sim_iccid
        List<Bson> pipeline = Arrays.asList(
                // 空的match条件，返回所有文档
                Aggregates.match(Filters.empty()),
                // 按update_time字段降序排序
                Aggregates.sort(Sorts.descending("update_time")),
                // 使用带有pipeline的lookup连接两个集合（左连接）
                Aggregates.lookup(
                        "traffic_info",// 连接的外部集合名称
                        Arrays.asList(
                                // 在lookup内部进行过滤
                                Aggregates.match(Filters.and(
                                        Filters.gte("traffic_time", a),
                                        Filters.lt("traffic_time", b),
                                        Filters.expr(Filters.eq("sim_iccid", "$sim_iccid")) // 使用$expr引用外部字段
                                )),
                                // 投影，排除不需要的字段
                                Aggregates.project(Projections.fields(
                                        Projections.excludeId(),
                                        Projections.exclude("create_time", "update_time")
                                )),
                                // 限制每个sim_iccid只返回一条记录
                                Aggregates.limit(1)
                        ),
                        "traffic_info_datas" // 连接结果存储在主文档中的字段名
                ),
                // 使用$mergeObjects和$first替换根文档，将traffic_info的字段合并到主文档
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
                                                "$traffic_info_datas" // 从traffic_info_datas字段（数组）中获取第一个元素
                                        )
                                )
                        )
                ),
                // 投影，排除不需要的字段
                Aggregates.project(Projections.fields(
                        Projections.excludeId(),
                        Projections.exclude("traffic_info_datas")
                )),
                // 跳过0条记录
                Aggregates.skip(0),
                // 限制返回结果数量为10个
                Aggregates.limit(10)
        );
        log.info("{}", mongoDBUtil.toAggregationsJson(pipeline));
        try (MongoCursor<Document> cursor = simInfoCollection.aggregate(pipeline).iterator();) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                log.info("{}", doc.toJson());
            }
        }
    }


    @Test
    void 分组卡商和卡状态() {
        List<Document> group = mongoDBUtil.group(new MongoQuery(simInfoCollection)
                .setGroupFields(Arrays.asList("api_type", "sim_status_raw", "sim_status"))
                .setTotalName("total")
                .setSort(Sorts.orderBy(
                        Sorts.ascending("api_type"),
                        Sorts.descending("total")
                ))
        );
        for (Document document : group) {
            log.info("卡商：{}，卡商返回状态：{}，导入状态：{}，总数：{}",
                    document.get("api_type"),
                    document.get("sim_status_raw"),
                    document.get("sim_status"),
                    document.get("total")
            );
        }
    }


    @Test
    void 测试写入map() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", true);
        map.put("c", null);
        map.put("d", "hello");
        map.put("e", LocalDateTime.now());
        InsertOneResult result = mongoDBUtil.insertOne(testCollection, map);
        log.info("{}", result);

        Map<String, Object> map2 = new HashMap<>();
        map2.putAll(map);
        map2.put("a", "2");

        Map<String, Object> map3 = new HashMap<>();
        map3.putAll(map);
        map3.put("a", "3");

        InsertManyResult insertManyResult = mongoDBUtil.insertMany(testCollection, Arrays.asList(map2, map3));
        log.info("{}", insertManyResult);
    }

    @Test
    void 测试写入实体() {
        SimInfo simInfo = new SimInfo();
        simInfo.setSimIccid("iccid1234567890");
        simInfo.setSimMsisdn("msisdn1234567890");
        simInfo.setDeviceId("did1234567890");
        simInfo.setDeviceCustomerName("张三");
        simInfo.setDeviceShippingTime(LocalDateTime.now());
        InsertOneResult result = mongoDBUtil.insertOneEntity(testCollection, simInfo);
        log.info("{}", result);

        SimInfo simInfo2 = new SimInfo();
        simInfo2.setSimIccid("iccid1234567891");
        simInfo2.setSimMsisdn("msisdn1234567891");
        simInfo2.setDeviceId("did1234567891");
        simInfo2.setDeviceCustomerName("李四");
        simInfo2.setDeviceShippingTime(LocalDateTime.now());

        SimInfo simInfo3 = new SimInfo();
        simInfo3.setSimIccid("iccid1234567892");
        simInfo3.setSimMsisdn("msisdn1234567892");
        simInfo3.setDeviceId("did1234567892");
        simInfo3.setDeviceCustomerName("王五");
        simInfo3.setDeviceShippingTime(LocalDateTime.now());

        InsertManyResult insertManyResult = mongoDBUtil.insertManyEntity(testCollection, Arrays.asList(simInfo2, simInfo3));
        log.info("{}", insertManyResult);
    }

    @Test
    void 测试saveOrUpdate() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        UpdateResult updateResult = mongoDBUtil.saveOrUpdate(testCollection, Filters.eq("sim_iccid", "iccid1234567890"), map);
        log.info("{}", updateResult);

        SimInfo simInfo3 = new SimInfo();
        simInfo3.setDeviceCustomerName("王五2");
        UpdateResult updateResult1 = mongoDBUtil.saveOrUpdateEntity(testCollection, Filters.eq("sim_iccid", "iccid1234567892"), simInfo3);
        log.info("{}", updateResult1);
    }

    @Test
    void 测试delete() {
        DeleteResult deleteResult = mongoDBUtil.delete(testCollection, Filters.eq("sim_iccid", "iccid1234567892"));
        log.info("{}", deleteResult);
    }

}