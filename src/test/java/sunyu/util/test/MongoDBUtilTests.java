package sunyu.util.test;

import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sunyu.util.Aggregations;
import sunyu.util.Expressions;
import sunyu.util.JsonUtil;
import sunyu.util.MongoDBUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * MongoDB工具类测试类
 * 用于测试MongoDBUtil工具类的各种功能，包括连接、查询、聚合等操作
 */
public class MongoDBUtilTests {
    // 日志记录器
    static Log log = LogFactory.get();
    // JSON工具类实例
    static JsonUtil jsonUtil;
    // MongoDB工具类实例
    static MongoDBUtil mongoDBUtil;
    // SIM数据库实例
    static MongoDatabase simDatabase;
    // SIM信息集合实例
    static MongoCollection<Document> simInfoCollection;

    /**
     * 在所有测试方法执行前运行
     * 初始化MongoDB连接和相关工具类
     */
    @BeforeAll
    static void beforeClass() {
        // 创建JSON工具类实例并设置时区为UTC
        jsonUtil = JsonUtil.builder().setTimeZone("UTC").build();
        // 创建MongoDB工具类实例并设置连接URI
        mongoDBUtil = MongoDBUtil.builder()
                // MongoDB连接字符串，包含用户名、密码、主机地址和连接参数
                .setUri("mongodb://root:Bcuser%262025@192.168.13.131:27000,192.168.13.133:27000/?authSource=admin&compressors=snappy,zlib,zstd&zlibCompressionLevel=9")
                .build();
        // 获取sim数据库实例
        simDatabase = mongoDBUtil.getDatabase("sim");
        // 获取sim_info集合实例
        simInfoCollection = mongoDBUtil.getCollection(simDatabase, "sim_info");
    }

    /**
     * 在所有测试方法执行后运行
     * 关闭MongoDB连接，释放资源
     */
    @AfterAll
    static void afterClass() {
        mongoDBUtil.close();
    }

    /**
     * 测试查找第一条记录
     * 验证能否成功连接数据库并获取第一条记录
     */
    @Test
    void testFindFirst() {
        // 查找集合中的第一条记录
        Document simInfo = simInfoCollection.find().first();
        // 使用MongoDB原生方式输出JSON
        log.info("{}", simInfo.toJson());
        // 使用自定义JSON工具类输出JSON
        log.info("{}", jsonUtil.objToJson(simInfo));
    }

    /**
     * 测试带条件和限制数量的查找
     * 验证按时间范围过滤并限制返回记录数量的功能
     */
    @Test
    void testFindLimit() {
        // 构造时间范围过滤条件：create_time在指定时间范围内
        Bson filter = Filters.and(
                // create_time大于指定时间
                Filters.gt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:00:00", "yyyy-MM-dd HH:mm:ss")),
                // create_time小于指定时间
                Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss"))
        );
        // 执行查找操作
        FindIterable<Document> list = simInfoCollection.find(filter);
        // 限制返回结果数量为10条
        list.limit(10);
        // 遍历并输出结果
        for (Document s : list) {
            log.info("{}", jsonUtil.objToJson(s));
        }
    }

    /**
     * 测试投影、排序和限制数量的查找
     * 验证字段投影、排序和限制数量的功能
     */
    @Test
    void testProjections() {
        // 执行查找操作
        FindIterable<Document> list = simInfoCollection.find();
        // 设置字段投影：排除_id字段，只包含sim_iccid和create_time字段
        list.projection(Projections.fields(
                Projections.excludeId(), // 排除_id字段
                Projections.include("sim_iccid", "create_time") // 包含指定字段
        ));
        // 按create_time字段降序排序
        list.sort(Sorts.descending("create_time"));
        // 限制返回结果数量为10条
        list.limit(10);
        // 遍历并输出结果
        for (Document doc : list) {
            log.info("{}", jsonUtil.objToJson(doc));
        }
    }

    /**
     * 测试使用游标方式查找
     * 验证使用游标遍历查询结果的功能
     */
    @Test
    void testFindByCursor() {
        // 使用try-with-resources确保游标正确关闭
        try (MongoCursor<Document> cursor = simInfoCollection
                // 构造时间范围过滤条件
                .find(Filters.and(
                        Filters.gt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:00:00", "yyyy-MM-dd HH:mm:ss")),
                        Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss"))
                ))
                // 获取游标
                .cursor()) {
            // 遍历游标中的所有文档
            while (cursor.hasNext()) {
                // 输出下一个文档
                log.info("{}", cursor.next().toJson());
            }
        }
    }

    /**
     * 测试聚合管道操作
     * 验证使用聚合管道进行复杂查询的功能
     */
    @Test
    void testAggregationWithFilter() {
        // 创建聚合管道列表
        List<Bson> pipeline = new ArrayList<>();
        // 添加匹配阶段：过滤create_time在指定时间范围内的文档
        pipeline.add(Aggregates.match(Filters.and(
                Filters.gt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:00:00", "yyyy-MM-dd HH:mm:ss")),
                Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss"))
        )));
        // 添加排序阶段：按create_time字段降序排序
        pipeline.add(Aggregates.sort(Sorts.descending("create_time")));
        // 添加投影阶段：排除_id字段，包含sim_iccid和create_time字段
        pipeline.add(Aggregates.project(Projections.fields(
                Projections.excludeId(), // 排除_id字段
                Projections.include("sim_iccid", "create_time") // 包含指定字段
        )));
        // 添加跳过阶段：跳过0条记录（此处无实际作用，仅作示例）
        pipeline.add(Aggregates.skip(0));
        // 添加限制阶段：限制返回结果数量为10条
        pipeline.add(Aggregates.limit(10));

        // 执行聚合管道并遍历结果
        for (Document doc : simInfoCollection.aggregate(pipeline)) {
            log.info("{}", jsonUtil.objToJson(doc));
        }
    }

    /**
     * 测试使用游标方式执行聚合管道
     * 验证使用游标遍历聚合结果的功能
     */
    @Test
    void testAggregationWithFilterCursor() {
        // 创建聚合管道列表
        List<Bson> pipeline = new ArrayList<>();
        // 添加匹配阶段：过滤create_time在指定时间范围内的文档
        pipeline.add(Aggregates.match(Filters.and(
                Filters.gt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:00:00", "yyyy-MM-dd HH:mm:ss")),
                Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss"))
        )));
        // 添加排序阶段：按create_time字段降序排序
        pipeline.add(Aggregates.sort(Sorts.descending("create_time")));
        // 添加投影阶段：排除_id字段，包含sim_iccid和create_time字段
        pipeline.add(Aggregates.project(Projections.fields(
                Projections.excludeId(), // 排除_id字段
                Projections.include("sim_iccid", "create_time") // 包含指定字段
        )));

        // 使用try-with-resources确保游标正确关闭
        // 执行聚合管道并获取游标
        try (MongoCursor<Document> cursor = simInfoCollection.aggregate(pipeline).cursor()) {
            // 遍历游标中的所有文档
            while (cursor.hasNext()) {
                // 获取下一个文档
                Document doc = cursor.next();
                // 输出文档
                log.info("{}", jsonUtil.objToJson(doc));
            }
        }
    }

    /**
     * 测试分组聚合操作
     * 验证按字段分组并重命名字段的功能
     */
    @Test
    void testGroup() {
        // 创建聚合管道列表
        List<Bson> pipeline = new ArrayList<>();
        // 添加分组阶段：按sim_status字段分组
        // 在聚合中，MongoDB会将分组字段放在_id字段中
        pipeline.add(Aggregations.group(MapUtil.of("status", "$sim_status")));
        // 添加投影阶段：重新组织输出字段
        pipeline.add(Aggregates.project(Projections.fields(
                Projections.excludeId(), // 排除_id字段
                // 使用计算字段将_id.status重命名为"卡状态"
                // $_id.status表示引用分组结果中_id对象的status字段
                Projections.computed("卡状态", "$_id.status")
        )));

        // 执行聚合管道并遍历结果
        for (Document doc : simInfoCollection.aggregate(pipeline)) {
            log.info("{}", jsonUtil.objToJson(doc));
        }
    }

    /**
     * 测试复杂分组聚合操作
     * 验证按字段分组、计数、排序和处理空值的功能
     */
    @Test
    void testGroup2() {
        // 创建聚合管道列表
        List<Bson> pipeline = new ArrayList<>();
        // 添加分组阶段：按device_customer_name字段分组，并统计每组的文档数量
        pipeline.add(
                Aggregations.group(MapUtil.of("customerName", "$device_customer_name"),
                        Accumulators.sum("count", 1))
        );
        // 添加排序阶段：先按count降序排序，再按_id.device_customer_name升序排序
        pipeline.add(Aggregates.sort(Sorts.orderBy(
                Sorts.descending("count"), // 按count字段降序排序
                Sorts.ascending("_id.customerName")) // 按_id.device_customer_name字段升序排序
        ));
        // 添加投影阶段：重新组织输出字段
        pipeline.add(Aggregates.project(Projections.fields(
                Projections.excludeId(), // 排除_id字段
                // 使用Expressions工具类的ifNull方法处理可能为空的字段
                // 如果$_id.device_customer_name字段为空，则显示"未添加"
                Projections.computed("客户名称",
                        Expressions.ifNull("$_id.customerName", "未添加")),
                Projections.include("count") // 包含count字段
        )));

        // 输出聚合管道的JSON表示，便于在MongoDB Shell中调试
        log.info("[{}] {}", simInfoCollection.getNamespace(), mongoDBUtil.toAggregationsJson(pipeline));

        // 执行聚合管道并遍历结果
        for (Document doc : simInfoCollection.aggregate(pipeline)) {
            log.info("{}", jsonUtil.objToJson(doc));
        }
    }

    /**
     * 测试计数操作
     * 验证统计满足条件的文档数量的功能
     */
    @Test
    void testCount() {
        // 统计sim_plan字段不存在或为null的文档数量
        long total = simInfoCollection.countDocuments(Filters.or(
                // sim_plan字段不存在
                Filters.not(Filters.exists("sim_plan")),
                // sim_plan字段为null
                Filters.eq("sim_plan", null)
        ));
        log.info("total: {}", total);
    }

    /**
     * 测试使用聚合管道计数
     * 验证使用聚合管道统计满足条件的文档数量的功能
     */
    @Test
    void testCountByAggregate() {
        // 创建聚合管道列表
        List<Bson> pipeline = new ArrayList<>();
        // 添加匹配阶段：过滤sim_plan字段不存在或为null的文档
        pipeline.add(Aggregates.match(Filters.or(
                Filters.not(Filters.exists("sim_plan")), // sim_plan字段不存在
                Filters.eq("sim_plan", null) // sim_plan字段为null
        )));
        // 添加计数阶段：统计匹配的文档数量
        pipeline.add(Aggregates.count("count"));
        // 添加投影阶段：排除_id字段
        pipeline.add(Aggregates.project(Projections.fields(
                Projections.excludeId() // 排除_id字段
        )));
        // 执行聚合管道并获取第一条结果
        Document result = simInfoCollection.aggregate(pipeline).first();
        // 如果有结果则输出计数，否则输出0
        if (result != null) {
            log.info("total: {}", result.values().iterator().next());
        } else {
            log.info("total: 0");
        }
    }

    /**
     * 测试统计所有文档数量
     * 验证统计集合中文档总数的功能
     */
    @Test
    void testCountAll() {
        // 统计集合中所有文档的数量
        long total = simInfoCollection.countDocuments();
        log.info("total: {}", total);
    }

    /**
     * 插入一个文档
     */
    @Test
    void insertOne() {
        HashMap<String, Object> m = new HashMap<>();
        simInfoCollection.insertOne(mongoDBUtil.mapToDocument(m));
    }

}
