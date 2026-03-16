# MongoDB工具类

## 描述

* 简化MongoDB操作流程

## 环境

* jdk8 x64 及以上版本

## 依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-mongodb</artifactId>
    <!-- {mongodb.driver.version}_{util.version}_{jdk.version} -->
    <version>5.6.4_1.0_jdk8</version>
    <classifier>shaded</classifier>
</dependency>
```

## 使用
如果是springboot项目，请排除自动配置功能
```
@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
```

## 例子

```java
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

public class SimInfo {
    @Column(column = "create_time", comment = "创建时间")
    private LocalDateTime createTime;

    @Column(column = "update_time", comment = "更新时间")
    private LocalDateTime updateTime;

    @Column(column = "sim_iccid", comment = "ICCID")
    private String simIccid;

    @Column(column = "sim_msisdn", comment = "SIM卡号")
    private String simMsisdn;

    @Column(column = "device_id", comment = "终端编号")
    private String deviceId;

    @Column(column = "device_customer_name", comment = "客户名称")
    private String deviceCustomerName;

    @Column(column = "device_shipping_time", comment = "发货时间")
    private LocalDateTime deviceShippingTime;

    @Column(column = "sim_status", comment = "SIM卡状态")
    private String simStatus;

    @Column(column = "sim_status_raw", comment = "运营商卡状态")
    private String simStatusRaw;

    @Column(column = "sim_activation_time", comment = "SIM卡激活时间")
    private LocalDateTime simActivationTime;

    @Column(column = "sim_deactivation_period", comment = "SIM卡注销时间")
    private LocalDateTime simDeactivationPeriod;

    @Column(column = "service_expiration_time", comment = "服务到期时间")
    private LocalDateTime serviceExpirationTime;

    @Column(column = "sim_operator", comment = "运营商")
    private String simOperator;

    @Column(column = "sim_plan", comment = "SIM卡套餐")
    private String simPlan;

    @Column(column = "sim_price", comment = "SIM卡单价")
    private String simPrice;

    @Column(column = "sim_purchase_time", comment = "SIM卡购买时间")
    private LocalDateTime simPurchaseTime;

    @Column(column = "device_imei", comment = "IMEI")
    private String deviceImei;

    @Column(column = "device_model", comment = "终端型号")
    private String deviceModel;

    @Column(column = "device_type", comment = "终端类型")
    private String deviceType;

    @Column(column = "device_protocol", comment = "通讯协议")
    private String deviceProtocol;

    @Column(column = "device_working_condition_id", comment = "工况ID")
    private String deviceWorkingConditionId;

    @Column(column = "device_logistics_no", comment = "物流单号")
    private String deviceLogisticsNo;

    @Column(column = "api_type", comment = "卡商")
    private String apiType;

    @Column(column = "api_sim_status_update_time", comment = "SIM卡状态最后同步时间")
    private LocalDateTime apiSimStatusUpdateTime;

    @Column(column = "api_sim_traffic_update_time", comment = "SIM卡流量最后同步时间")
    private LocalDateTime apiSimTrafficUpdateTime;

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public String getSimIccid() {
        return simIccid;
    }

    public void setSimIccid(String simIccid) {
        this.simIccid = simIccid;
    }

    public String getSimMsisdn() {
        return simMsisdn;
    }

    public void setSimMsisdn(String simMsisdn) {
        this.simMsisdn = simMsisdn;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceCustomerName() {
        return deviceCustomerName;
    }

    public void setDeviceCustomerName(String deviceCustomerName) {
        this.deviceCustomerName = deviceCustomerName;
    }

    public LocalDateTime getDeviceShippingTime() {
        return deviceShippingTime;
    }

    public void setDeviceShippingTime(LocalDateTime deviceShippingTime) {
        this.deviceShippingTime = deviceShippingTime;
    }

    public String getSimStatus() {
        return simStatus;
    }

    public void setSimStatus(String simStatus) {
        this.simStatus = simStatus;
    }

    public String getSimStatusRaw() {
        return simStatusRaw;
    }

    public void setSimStatusRaw(String simStatusRaw) {
        this.simStatusRaw = simStatusRaw;
    }

    public LocalDateTime getSimActivationTime() {
        return simActivationTime;
    }

    public void setSimActivationTime(LocalDateTime simActivationTime) {
        this.simActivationTime = simActivationTime;
    }

    public LocalDateTime getSimDeactivationPeriod() {
        return simDeactivationPeriod;
    }

    public void setSimDeactivationPeriod(LocalDateTime simDeactivationPeriod) {
        this.simDeactivationPeriod = simDeactivationPeriod;
    }

    public LocalDateTime getServiceExpirationTime() {
        return serviceExpirationTime;
    }

    public void setServiceExpirationTime(LocalDateTime serviceExpirationTime) {
        this.serviceExpirationTime = serviceExpirationTime;
    }

    public String getSimOperator() {
        return simOperator;
    }

    public void setSimOperator(String simOperator) {
        this.simOperator = simOperator;
    }

    public String getSimPlan() {
        return simPlan;
    }

    public void setSimPlan(String simPlan) {
        this.simPlan = simPlan;
    }

    public String getSimPrice() {
        return simPrice;
    }

    public void setSimPrice(String simPrice) {
        this.simPrice = simPrice;
    }

    public LocalDateTime getSimPurchaseTime() {
        return simPurchaseTime;
    }

    public void setSimPurchaseTime(LocalDateTime simPurchaseTime) {
        this.simPurchaseTime = simPurchaseTime;
    }

    public String getDeviceImei() {
        return deviceImei;
    }

    public void setDeviceImei(String deviceImei) {
        this.deviceImei = deviceImei;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getDeviceProtocol() {
        return deviceProtocol;
    }

    public void setDeviceProtocol(String deviceProtocol) {
        this.deviceProtocol = deviceProtocol;
    }

    public String getDeviceWorkingConditionId() {
        return deviceWorkingConditionId;
    }

    public void setDeviceWorkingConditionId(String deviceWorkingConditionId) {
        this.deviceWorkingConditionId = deviceWorkingConditionId;
    }

    public String getDeviceLogisticsNo() {
        return deviceLogisticsNo;
    }

    public void setDeviceLogisticsNo(String deviceLogisticsNo) {
        this.deviceLogisticsNo = deviceLogisticsNo;
    }

    public String getApiType() {
        return apiType;
    }

    public void setApiType(String apiType) {
        this.apiType = apiType;
    }

    public LocalDateTime getApiSimStatusUpdateTime() {
        return apiSimStatusUpdateTime;
    }

    public void setApiSimStatusUpdateTime(LocalDateTime apiSimStatusUpdateTime) {
        this.apiSimStatusUpdateTime = apiSimStatusUpdateTime;
    }

    public LocalDateTime getApiSimTrafficUpdateTime() {
        return apiSimTrafficUpdateTime;
    }

    public void setApiSimTrafficUpdateTime(LocalDateTime apiSimTrafficUpdateTime) {
        this.apiSimTrafficUpdateTime = apiSimTrafficUpdateTime;
    }
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
```

