package sunyu.util.query;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongoQuery {
    private MongoCollection<Document> collection;

    private Bson filter = Filters.empty();
    private Bson projection;
    private Bson sort;
    private Integer skip;
    private Integer limit;

    private List<String> groupFields = new ArrayList<>();
    private String totalName;

    private String rightCollectionName;
    private String leftJoinField;
    private String rightJoinField;
    private Bson rightProjection;
    private Integer rightLimit = 1;
    private Bson rightFilter = Filters.empty();
    private Boolean mergeRightObjectsToLeft;

    /**
     * 创建一个查询对象
     *
     * @param collection 查询的集合
     */
    public MongoQuery(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    public MongoCollection<Document> getCollection() {
        return collection;
    }

    /**
     * 设置查询的集合
     *
     * @param collection 查询的集合
     * @return 当前查询对象
     */
    public MongoQuery setCollection(MongoCollection<Document> collection) {
        this.collection = collection;
        return this;
    }

    public Bson getFilter() {
        return filter;
    }

    /**
     * 设置查询条件
     * <pre>
     * Filters.or(
     *      // 字段不存在
     *      Filters.exists("sim_msisdn", false),
     *      // 字段值为null
     *      Filters.eq("sim_msisdn", null),
     *      // 字段为空字符串或仅包含空白字符
     *      Filters.regex("sim_msisdn", "^\\s*$")
     * )
     * </pre>
     *
     * <pre>
     * Filters.and(
     *      Filters.gte("traffic_time", a),
     *      Filters.lt("traffic_time", b)
     * )
     * </pre>
     *
     * @param filter 查询条件
     * @return 当前查询对象
     */
    public MongoQuery setFilter(Bson filter) {
        this.filter = filter;
        return this;
    }

    public Integer getSkip() {
        return skip;
    }

    /**
     * 设置跳过的记录数
     * <pre>
     * (page - 1) * pageSize
     * </pre>
     *
     * @param skip 跳过的记录数(从0开始)
     * @return 当前查询对象
     */
    public MongoQuery setSkip(Integer skip) {
        this.skip = skip;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    /**
     * 设置返回的记录数
     *
     * @param limit 返回的记录数
     * @return 当前查询对象
     */
    public MongoQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 设置查询第几页和每页返回的记录数
     *
     * @param page     第几页(从1开始)
     * @param pageSize 返回的记录数
     * @return 当前查询对象
     */
    public MongoQuery setPage(Integer page, Integer pageSize) {
        this.skip = (page - 1) * pageSize;
        this.limit = pageSize;
        return this;
    }

    public Bson getProjection() {
        return projection;
    }

    /**
     *
     * 设置投影字段
     * <pre>
     * Projections.fields(Projections.include("sim_msisdn", "sim_status")))
     * </pre>
     * <pre>
     * Projections.fields(
     *      Projections.excludeId(),
     *      Projections.computed("status", "$_id"),
     *      Projections.include("count")
     * )
     * </pre>
     *
     * @param projection 投影字段
     * @return 当前查询对象
     */
    public MongoQuery setProjection(Bson projection) {
        this.projection = projection;
        return this;
    }

    public Bson getSort() {
        return sort;
    }

    /**
     * 设置排序字段
     * <pre>
     * Sorts.orderBy(Sorts.descending("update_time"))
     * </pre>
     * <pre>
     * Sorts.orderBy(
     *      Sorts.descending("update_time"),
     *      Sorts.ascending("create_time")
     * )
     * </pre>
     *
     * @param sort 排序字段
     * @return 当前查询对象
     */
    public MongoQuery setSort(Bson sort) {
        this.sort = sort;
        return this;
    }

    public List<String> getGroupFields() {
        return groupFields;
    }

    /**
     * 设置分组字段
     *
     * @param groupFields 分组字段
     * @return 当前查询对象
     */
    public MongoQuery setGroupFields(List<String> groupFields) {
        this.groupFields = groupFields;
        return this;
    }

    /**
     * 设置分组字段
     *
     * @param groupField 分组字段
     * @return 当前查询对象
     */
    public MongoQuery setGroupField(String groupField) {
        this.groupFields = Collections.singletonList(groupField);
        return this;
    }

    public String getTotalName() {
        return totalName;
    }

    /**
     * 设置分组字段的统计字段名
     *
     * @param totalName 统计字段名，如果为null则不做分组明细统计
     * @return 当前查询对象
     */
    public MongoQuery setTotalName(String totalName) {
        this.totalName = totalName;
        return this;
    }

    public String getRightCollectionName() {
        return rightCollectionName;
    }

    /**
     * 设置右表集合名称
     *
     * @param rightCollectionName 右表集合名称
     * @return 当前查询对象
     */
    public MongoQuery setRightCollectionName(String rightCollectionName) {
        this.rightCollectionName = rightCollectionName;
        return this;
    }

    public String getLeftJoinField() {
        return leftJoinField;
    }

    /**
     * 设置左表连接字段
     *
     * @param leftJoinField 左表连接字段
     * @return 当前查询对象
     */
    public MongoQuery setLeftJoinField(String leftJoinField) {
        this.leftJoinField = leftJoinField;
        return this;
    }

    public String getRightJoinField() {
        return rightJoinField;
    }

    /**
     * 设置右表连接字段
     *
     * @param rightJoinField 右表连接字段
     * @return 当前查询对象
     */
    public MongoQuery setRightJoinField(String rightJoinField) {
        this.rightJoinField = rightJoinField;
        return this;
    }

    public Bson getRightProjection() {
        return rightProjection;
    }

    /**
     * 设置右表投影字段
     *
     * @param rightProjection 右表投影字段
     * @return 当前查询对象
     */
    public MongoQuery setRightProjection(Bson rightProjection) {
        this.rightProjection = rightProjection;
        return this;
    }

    public Integer getRightLimit() {
        return rightLimit;
    }

    /**
     * 设置右表返回的记录数
     *
     * @param rightLimit 右表返回的记录数
     * @return 当前查询对象
     */
    public MongoQuery setRightLimit(Integer rightLimit) {
        this.rightLimit = rightLimit;
        return this;
    }


    public Bson getRightFilter() {
        return rightFilter;
    }

    /**
     * 设置右表连接过滤条件
     *
     * @param rightFilter 右表连接过滤条件
     * @return 当前查询对象
     */
    public MongoQuery setRightFilter(Bson rightFilter) {
        this.rightFilter = rightFilter;
        return this;
    }

    public Boolean getMergeRightObjectsToLeft() {
        return mergeRightObjectsToLeft;
    }

    /**
     * 设置是否将右表的第一条记录合并到左表中
     *
     * @param mergeRightObjectsToLeft 是否合并到左表
     * @return 当前查询对象
     */
    public MongoQuery setMergeRightObjectsToLeft(Boolean mergeRightObjectsToLeft) {
        this.mergeRightObjectsToLeft = mergeRightObjectsToLeft;
        return this;
    }
}