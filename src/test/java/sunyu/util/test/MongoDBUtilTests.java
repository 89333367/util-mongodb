package sunyu.util.test;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.ttzero.excel.reader.ExcelReader;
import org.ttzero.excel.reader.Row;
import sunyu.util.Aggregations;
import sunyu.util.Expressions;
import sunyu.util.JsonUtil;
import sunyu.util.MongoDBUtil;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;

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

    static MongoDatabase nrvpDatabase;
    static MongoCollection<Document> vcCollection;

    static List<String> dateFormats = Arrays.asList(
            "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SS", "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss",
            "yyyy-M-d HH:mm:ss", "yyyy/M/d HH:mm:ss",
            "yyyy-MM-dd", "yyyy/MM/dd",
            "yyyy-M-d", "yyyy/M/d",
            "yyyy年M月d日", "yyyy年MM月dd日"
    );

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
                .setUri("mongodb://bcuser:Bcld&2025@192.168.13.134:27017/?authSource=sim&compressors=snappy,zlib,zstd&zlibCompressionLevel=9")
                //.setUri("mongodb://root:Bcuser%262025@192.168.13.131:27000,192.168.13.133:27000/?authSource=admin&compressors=snappy,zlib,zstd&zlibCompressionLevel=9")
                //.setUri("mongodb://bcuser:Bcld%262025@123.124.91.28:19700/?authSource=sim&compressors=snappy,zlib,zstd&zlibCompressionLevel=9")
                .build();
        // 获取sim数据库实例
        simDatabase = mongoDBUtil.getDatabase("sim");
        // 获取sim_info集合实例
        simInfoCollection = mongoDBUtil.getCollection(simDatabase, "sim_info");

        nrvpDatabase = mongoDBUtil.getDatabase("nrvp");
        vcCollection = nrvpDatabase.getCollection("v_c");
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
    void 查找sim卡号() {
        String sim_msisdn = "1440000584570";
        long count = simInfoCollection.countDocuments(Filters.eq("sim_msisdn", sim_msisdn));
        log.debug("{} 找到 {} 条记录", sim_msisdn, count);
        if (count == 1) {
            Document simInfo = simInfoCollection.find(Filters.eq("sim_msisdn", sim_msisdn)).first();
            log.debug("{}", jsonUtil.objToJson(simInfo));
        } else if (count > 1) {
            log.error("{} 找到多条", sim_msisdn);
        } else {
            log.info("{} 未找到", sim_msisdn);
        }
    }

    @Test
    void testQuery() {
        int i = 0;
        for (String vin : FileUtil.readUtf8Lines("d:/tmp/1.txt")) {
            Document v = vcCollection.find(Filters.eq("vin", vin)).first();
            log.info("{} {}", vin, v);
            if (v == null) {
                i++;
            }
        }
        log.info("{}", i);
    }

    /**
     * 测试查找第一条记录
     * 验证能否成功连接数据库并获取第一条记录
     */
    @Test
    void testFindFirst() {
        // 查找集合中的第一条记录
        Document simInfo = simInfoCollection.find(Filters.eq("device_id", "XDR13102509200055")).first();
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
                Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss")));
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
                        Filters.gt("create_time",
                                LocalDateTimeUtil.parse("2025-05-09 03:00:00", "yyyy-MM-dd HH:mm:ss")),
                        Filters.lt("create_time",
                                LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss"))))
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
                Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss")))));
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
                Filters.lt("create_time", LocalDateTimeUtil.parse("2025-05-09 03:40:00", "yyyy-MM-dd HH:mm:ss")))));
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
                Projections.computed("卡状态", "$_id.status"))));

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
                        Accumulators.sum("count", 1)));
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

    @Test
    void testGroup3() {
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.match(Filters.and(
                Filters.gt("gps_time", LocalDateTimeUtil.parse("2024-01-01 03:00:00", "yyyy-MM-dd HH:mm:ss")),
                Filters.lt("gps_time", LocalDateTimeUtil.parse("2025-02-01 03:40:00", "yyyy-MM-dd HH:mm:ss")))));
        pipeline.add(
                Aggregations.group(MapUtil.of("did", "$did"),
                        Accumulators.sum("count", 1)));
        pipeline.add(Aggregates.sort(Sorts.orderBy(
                Sorts.descending("count"), // 按count字段降序排序
                Sorts.ascending("_id.did")) // 按_id.device_customer_name字段升序排序
        ));
        pipeline.add(Aggregates.project(Projections.fields(
                Projections.excludeId(), // 排除_id字段
                Projections.computed("设备号",
                        Expressions.ifNull("$_id.did", "无")),
                Projections.include("count") // 包含count字段
        )));
        log.info("[{}] {}", vcCollection.getNamespace(), mongoDBUtil.toAggregationsJson(pipeline));
        for (Document doc : vcCollection.aggregate(pipeline)) {
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
                Filters.eq("sim_plan", null)));
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
    void testInsertOne() {
        Map<String, Object> m = new HashMap<>();
        InsertOneResult insertOneResult = mongoDBUtil.insertOne(simInfoCollection, m);
        log.info("{}", insertOneResult);
    }

    @Test
    void testUpdate() {
        Bson filter = Filters.eq("sim_iccid", "2");
        Document simIccid = simInfoCollection.find(filter).first();
        log.info("{}", jsonUtil.objToJson(simIccid));
        if (simIccid != null) {
            UpdateResult updateResult = mongoDBUtil.saveOrUpdate(simInfoCollection, filter, new HashMap<String, Object>() {{
                put("sim_msisdn", "2");
            }});
            log.info("{}", updateResult);
        }
    }

    LocalDateTime getLocalDateTime(Object value) {
        String v = Convert.toStr(value, null);
        if (StrUtil.isNotBlank(v)) {
            v = v.trim();
            for (String dateFormat : dateFormats) {
                try {
                    return LocalDateTimeUtil.parse(v, dateFormat);
                } catch (Exception e) {
                }
            }
            throw new RuntimeException("日期格式错误：" + v);
        }
        return null;
    }

    @Test
    void 数据排查() {
        try (ExcelReader reader = ExcelReader.read(Paths.get("D:/tmp/发货记录汇总/2025年合 - 终版.xlsx"))) {
            reader.sheet(0).header(1).rows().map(Row::toMap).forEach(row -> {
                String sim_iccid = Convert.toStr(row.get("ICCID(不可重复)"), null);
                String sim_msisdn = Convert.toStr(row.get("SIM卡号(不可重复)"), null);
                String device_id = Convert.toStr(row.get("终端编号（必填）"), null);
                String device_working_condition_id = Convert.toStr(row.get("工况ID(不可重复)"), null);
                String device_imei = Convert.toStr(row.get("IMEI(不可重复)"), null);
                LocalDateTime device_shipping_time = getLocalDateTime(row.get("发货时间(必填)"));
                String device_customer_name = Convert.toStr(row.get("客户名称(必填)"), null);
                String device_model = Convert.toStr("终端型号", null);
                String device_type = Convert.toStr("终端类型", null);
                LocalDateTime service_expiration_time = getLocalDateTime(row.get("服务到期时间"));
                String device_logistics_no = Convert.toStr(row.get("物流单号"), null);
                Document simInfo = null;
                long count;
                if (StrUtil.isNotBlank(sim_iccid)) {
                    sim_iccid = sim_iccid.trim();
                    if (sim_iccid.length() != 19 && sim_iccid.length() != 20) {
                        log.error("sim_iccid {} 长度错误", sim_iccid);
                        return;
                    }
                    Bson filter = Filters.eq("sim_iccid", sim_iccid.trim());
                    count = simInfoCollection.countDocuments(filter);
                    if (count == 0) {
                        //log.warn("找不到 sim_iccid {}，准备新增", sim_iccid);
                        simInfo = new Document();
                        simInfo.put("sim_iccid", sim_iccid.trim());
                    } else {
                        simInfo = simInfoCollection.find(filter).first();
                        //log.debug("通过sim_iccid找到了 {}", simInfo);
                    }
                }
                if (simInfo == null && StrUtil.isNotBlank(sim_msisdn)) {
                    sim_msisdn = sim_msisdn.trim();
                    Bson filter = Filters.eq("sim_msisdn", sim_msisdn.trim());
                    count = simInfoCollection.countDocuments(filter);
                    if (count == 0) {
                        log.error("找不到 sim_msisdn {}", sim_msisdn);
                        return;
                    } else if (count > 1) {
                        log.error("sim_msisdn {} 重复", sim_msisdn);
                        return;
                    }
                    simInfo = simInfoCollection.find(filter).first();
                    //log.debug("通过sim_msisdn找到了 {}", simInfo);
                }
                if (simInfo == null) {
                    log.error("输入参数错误 sim_iccid {} sim_msisdn {} device_id {} device_imei {} device_working_condition_id {}", sim_iccid, sim_msisdn, device_id, device_imei, device_working_condition_id);
                    return;
                }
            });
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }
        log.info("done");
    }

    @Test
    void 刷写数据() {
        try (ExcelReader reader = ExcelReader.read(Paths.get("D:/tmp/发货记录汇总/2025年合 - 终版.xlsx"))) {
            reader.sheet(0).header(1).rows().map(Row::toMap).forEach(row -> {
                log.debug("准备处理 {}", row);
                String sim_iccid = Convert.toStr(row.get("ICCID(不可重复)"), null);
                String sim_msisdn = Convert.toStr(row.get("SIM卡号(不可重复)"), null);
                String device_id = Convert.toStr(row.get("终端编号（必填）"), null);
                String device_working_condition_id = Convert.toStr(row.get("工况ID(不可重复)"), null);
                String device_imei = Convert.toStr(row.get("IMEI(不可重复)"), null);
                LocalDateTime device_shipping_time = getLocalDateTime(row.get("发货时间(必填)"));
                String device_customer_name = Convert.toStr(row.get("客户名称(必填)"), null);
                String device_model = Convert.toStr("终端型号", null);
                String device_type = Convert.toStr("终端类型", null);
                LocalDateTime service_expiration_time = getLocalDateTime(row.get("服务到期时间"));
                String device_logistics_no = Convert.toStr(row.get("物流单号"), null);
                Document simInfo = null;
                long count;
                if (StrUtil.isNotBlank(sim_iccid)) {
                    sim_iccid = sim_iccid.trim();
                    if (sim_iccid.length() != 19 && sim_iccid.length() != 20) {
                        log.error("sim_iccid {} 长度错误", sim_iccid);
                        return;
                    }
                    Bson filter = Filters.eq("sim_iccid", sim_iccid);
                    count = simInfoCollection.countDocuments(filter);
                    if (count == 0) {
                        log.warn("找不到 sim_iccid {}，准备新增", sim_iccid);
                        simInfo = new Document();
                        simInfo.put("sim_iccid", sim_iccid);
                    } else {
                        simInfo = simInfoCollection.find(filter).first();
                        log.debug("通过sim_iccid找到了 {}", simInfo);
                    }
                }
                if (simInfo == null && StrUtil.isNotBlank(sim_msisdn)) {
                    sim_msisdn = sim_msisdn.trim();
                    Bson filter = Filters.eq("sim_msisdn", sim_msisdn);
                    count = simInfoCollection.countDocuments(filter);
                    if (count == 0) {
                        log.error("找不到 sim_msisdn {}", sim_msisdn);
                        return;
                    } else if (count > 1) {
                        log.error("sim_msisdn {} 重复", sim_msisdn);
                        return;
                    }
                    simInfo = simInfoCollection.find(filter).first();
                    log.debug("通过sim_msisdn找到了 {}", simInfo);
                }
                if (simInfo == null) {
                    log.error("输入参数错误 sim_iccid {} sim_msisdn {} device_id {} device_imei {} device_working_condition_id {}", sim_iccid, sim_msisdn, device_id, device_imei, device_working_condition_id);
                    return;
                }

                Map<String, Object> map = new HashMap<>();
                if (!simInfo.containsKey("sim_msisdn") && StrUtil.isNotBlank(sim_msisdn)) {
                    map.put("sim_msisdn", sim_msisdn.trim());
                }
                if (!simInfo.containsKey("device_id") && StrUtil.isNotBlank(device_id)) {
                    map.put("device_id", device_id.trim());
                }
                if (!simInfo.containsKey("device_working_condition_id") && StrUtil.isNotBlank(device_working_condition_id)) {
                    map.put("device_working_condition_id", device_working_condition_id.trim());
                }
                if (!simInfo.containsKey("device_imei") && StrUtil.isNotBlank(device_imei)) {
                    map.put("device_imei", device_imei.trim());
                }
                if (!simInfo.containsKey("device_shipping_time") && device_shipping_time != null) {
                    map.put("device_shipping_time", device_shipping_time);
                }
                if (!simInfo.containsKey("device_customer_name") && StrUtil.isNotBlank(device_customer_name)) {
                    map.put("device_customer_name", device_customer_name.trim());
                }
                if (!simInfo.containsKey("device_model") && StrUtil.isNotBlank(device_model)) {
                    map.put("device_model", device_model.trim());
                }
                if (!simInfo.containsKey("device_type") && StrUtil.isNotBlank(device_type)) {
                    map.put("device_type", device_type.trim());
                }
                if (!simInfo.containsKey("service_expiration_time") && service_expiration_time != null) {
                    map.put("service_expiration_time", service_expiration_time);
                }
                if (!simInfo.containsKey("device_logistics_no") && StrUtil.isNotBlank(device_logistics_no)) {
                    map.put("device_logistics_no", device_logistics_no.trim());
                }
                if (MapUtil.isNotEmpty(map)) {
                    UpdateResult updateResult = mongoDBUtil.saveOrUpdate(simInfoCollection, Filters.eq("sim_iccid", simInfo.getString("sim_iccid")), map);
                    log.debug("更新结果 {}", updateResult);
                }
            });
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }
        log.info("done");
    }

}