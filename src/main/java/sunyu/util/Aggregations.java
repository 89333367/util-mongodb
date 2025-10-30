package sunyu.util;

import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;

/**
 * 聚合管道扩展工具类
 *
 * <p>该类提供了构建MongoDB聚合管道的便捷方法，是对MongoDB原生Aggregates类的扩展。
 * 主要用于简化聚合管道的构建，避免直接使用Document操作。</p>
 *
 * @author SunYu
 * @since 1.0
 */
public final class Aggregations {
    private Aggregations() {
        // 私有构造器，防止实例化
    }

    /**
     * 带别名的分组
     * 示例：group(Map.of("customer", "$device_customer_name", "status", "$sim_status"))
     * -> { $group: { _id: { customer: "$device_customer_name", status: "$sim_status" } } }
     */
    public static Bson group(Map<String, String> fieldAliases) {
        Document idDoc = new Document();
        for (Map.Entry<String, String> entry : fieldAliases.entrySet()) {
            idDoc.append(entry.getKey(), entry.getValue());
        }
        return Aggregates.group(idDoc);
    }

    /**
     * 带别名的分组 + 累加器
     * 示例：group(MapUtil.of("customer", "$device_customer_name"), Accumulators.sum("count", 1))
     * -> { $group: { _id: { customer: "$device_customer_name" }, count: { $sum: 1 } } }
     */
    public static Bson group(Map<String, String> fieldAliases, BsonField... accumulators) {
        Document idDoc = new Document();
        for (Map.Entry<String, String> entry : fieldAliases.entrySet()) {
            idDoc.append(entry.getKey(), entry.getValue());
        }
        return Aggregates.group(idDoc, accumulators);
    }
}
