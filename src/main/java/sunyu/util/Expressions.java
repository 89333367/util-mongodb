package sunyu.util;

import org.bson.Document;

import java.util.Arrays;
import java.util.Collections;

/**
 * 聚合表达式工具类
 *
 * <p>该类提供了构建MongoDB聚合管道表达式的便捷方法，类似于Filters类的使用方式。
 * 主要用于在聚合管道的$project、$group、$cond等阶段构建表达式。</p>
 *
 * <p>使用示例：</p>
 * <pre>
 * // 在聚合管道中使用
 * List<Bson> pipeline = Arrays.asList(
 *     project(Projections.fields(
 *         Projections.computed("customerName",
 *             Expressions.ifNull("$_id.device_customer_name", "未添加")),
 *         Projections.computed("statusDesc",
 *             Expressions.cond(
 *                 Expressions.eq("$status", "active"),
 *                 "活跃",
 *                 "非活跃"
 *             ))
 *     ))
 * );
 * </pre>
 *
 * @author SunYu
 * @since 1.0
 */
public class Expressions {
    private Expressions() {
        // 私有构造器，防止实例化
    }

    /**
     * $ifNull 表达式: { $ifNull: [ <expression>, <replacement-expression-if-null> ] }
     *
     * <p>如果第一个表达式的值不为null，则返回该值；否则返回第二个表达式的值。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 如果 device_customer_name 为 null，则返回 "未添加"
     * Expressions.ifNull("$_id.device_customer_name", "未添加")
     * </pre>
     *
     * @param field        要检查的字段表达式
     * @param defaultValue 当字段为null时的默认值
     *
     * @return 包含$ifNull表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/ifnull/">MongoDB $ifNull 文档</a>
     */
    public static Document ifNull(Object field, Object defaultValue) {
        return new Document("$ifNull", Arrays.asList(field, defaultValue));
    }

    /**
     * $concat 表达式: { $concat: [ <expression1>, <expression2>, ... ] }
     *
     * <p>连接多个字符串表达式。如果任何表达式为null，则整个结果为null。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 连接姓和名
     * Expressions.concat("$firstName", " ", "$lastName")
     * </pre>
     *
     * @param values 要连接的表达式数组
     *
     * @return 包含$concat表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/concat/">MongoDB $concat 文档</a>
     */
    public static Document concat(Object... values) {
        return new Document("$concat", Arrays.asList(values));
    }

    /**
     * $eq 表达式: { $eq: [ <expression1>, <expression2> ] }
     *
     * <p>比较两个表达式是否相等。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 比较状态是否为"active"
     * Expressions.eq("$status", "active")
     * </pre>
     *
     * @param expr1 第一个表达式
     * @param expr2 第二个表达式
     *
     * @return 包含$eq表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/eq/">MongoDB $eq 文档</a>
     */
    public static Document eq(Object expr1, Object expr2) {
        return new Document("$eq", Arrays.asList(expr1, expr2));
    }

    /**
     * $cond 表达式: { $cond: [ <boolean-expression>, <true-expression>, <false-expression> ] }
     *
     * <p>条件表达式：如果第一个表达式为true，则返回第二个表达式的值，否则返回第三个表达式的值。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 如果年龄大于等于18则返回"成年人"，否则返回"未成年人"
     * Expressions.cond(
     *     Expressions.gte("$age", 18),
     *     "成年人",
     *     "未成年人"
     * )
     * </pre>
     *
     * @param condition  布尔条件表达式
     * @param trueValue  条件为true时的返回值
     * @param falseValue 条件为false时的返回值
     *
     * @return 包含$cond表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/cond/">MongoDB $cond 文档</a>
     */
    public static Document cond(Object condition, Object trueValue, Object falseValue) {
        return new Document("$cond", Arrays.asList(condition, trueValue, falseValue));
    }

    /**
     * $dateToString 表达式: { $dateToString: { format: <formatString>, date: <dateExpression> } }
     *
     * <p>将日期对象格式化为字符串。</p>
     *
     * <p>常用日期格式说明：</p>
     * <ul>
     *   <li>%Y - 完整年份 (例如: 2025)</li>
     *   <li>%y - 年份后两位 (例如: 25)</li>
     *   <li>%m - 月份 (01-12)</li>
     *   <li>%d - 日期 (01-31)</li>
     *   <li>%H - 小时 (00-23)</li>
     *   <li>%M - 分钟 (00-59)</li>
     *   <li>%S - 秒 (00-59)</li>
     *   <li>%L - 毫秒 (000-999)</li>
     *   <li>%j - 一年中的第几天 (001-366)</li>
     *   <li>%w - 一周中的第几天 (1-7, Sunday=1)</li>
     *   <li>%U - 一年中的第几周 (00-53, Sunday为一周开始)</li>
     *   <li>%G - 基于周的年份</li>
     *   <li>%V - 基于周的年份中的第几周 (1-53)</li>
     * </ul>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 将创建时间格式化为"YYYY-MM-DD"格式
     * Expressions.dateToString("$create_time", "%Y-%m-%d")
     *
     * // 格式化为"YYYY-MM-DD HH:MM:SS"格式
     * Expressions.dateToString("$create_time", "%Y-%m-%d %H:%M:%S")
     *
     * // 只显示时间部分
     * Expressions.dateToString("$create_time", "%H:%M:%S")
     * </pre>
     *
     * @param dateField 日期字段表达式
     * @param format    日期格式字符串，遵循MongoDB日期格式规范
     *
     * @return 包含$dateToString表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/datetostring/">MongoDB $dateToString 文档</a>
     */
    public static Document dateToString(String dateField, String format) {
        return new Document("$dateToString",
                new Document("format", format).append("date", dateField));
    }

    /**
     * $toUpper 表达式: { $toUpper: <expression> }
     *
     * <p>将字符串表达式转换为大写。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 将姓名转换为大写
     * Expressions.toUpper("$name")
     * </pre>
     *
     * @param field 字符串字段表达式
     *
     * @return 包含$toUpper表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/toUpper/">MongoDB $toUpper 文档</a>
     */
    public static Document toUpper(String field) {
        return new Document("$toUpper", field);
    }

    /**
     * $toLower 表达式: { $toLower: <expression> }
     *
     * <p>将字符串表达式转换为小写。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 将姓名转换为小写
     * Expressions.toLower("$name")
     * </pre>
     *
     * @param field 字符串字段表达式
     *
     * @return 包含$toLower表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/toLower/">MongoDB $toLower 文档</a>
     */
    public static Document toLower(String field) {
        return new Document("$toLower", field);
    }

    /**
     * $in 表达式: { $in: [ <expression>, <array expression> ] }
     *
     * <p>检查表达式的值是否在数组中。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 检查状态是否为active或pending
     * Expressions.in("$status", Arrays.asList("active", "pending"))
     * </pre>
     *
     * @param expr      要检查的表达式
     * @param arrayExpr 数组表达式
     *
     * @return 包含$in表达式的Document
     */
    public static Document in(Object expr, Object arrayExpr) {
        return new Document("$in", Arrays.asList(expr, arrayExpr));
    }

    /**
     * $nin 表达式: { $nin: [ <expression>, <array expression> ] }
     *
     * <p>检查表达式的值是否不在数组中。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 检查状态是否不为deleted或suspended
     * Expressions.nin("$status", Arrays.asList("deleted", "suspended"))
     * </pre>
     *
     * @param expr      要检查的表达式
     * @param arrayExpr 数组表达式
     *
     * @return 包含$nin表达式的Document
     */
    public static Document nin(Object expr, Object arrayExpr) {
        return new Document("$nin", Arrays.asList(expr, arrayExpr));
    }

    /**
     * $exists 表达式: { $exists: [ <expression>, <boolean> ] }
     *
     * <p>检查字段是否存在。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 检查device_id字段是否存在
     * Expressions.exists("$device_id", true)
     * </pre>
     *
     * @param expr   字段表达式
     * @param exists true表示检查字段存在，false表示检查字段不存在
     *
     * @return 包含$exists表达式的Document
     */
    public static Document exists(Object expr, boolean exists) {
        return new Document("$exists", Arrays.asList(expr, exists));
    }

    /**
     * $size 表达式: { $size: [ <expression> ] }
     *
     * <p>返回数组的大小。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取tags数组的大小
     * Expressions.size("$tags")
     * </pre>
     *
     * @param expr 数组字段表达式
     *
     * @return 包含$size表达式的Document
     */
    public static Document size(Object expr) {
        return new Document("$size", Collections.singletonList(expr));
    }

    /**
     * $substr 表达式: { $substr: [ <string>, <start>, <length> ] }
     *
     * <p>从字符串中提取子字符串。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 从手机号码中提取前3位
     * Expressions.substr("$phone", 0, 3)
     * </pre>
     *
     * @param string 字符串表达式
     * @param start  开始位置（从0开始）
     * @param length 提取长度
     *
     * @return 包含$substr表达式的Document
     */
    public static Document substr(Object string, Object start, Object length) {
        return new Document("$substr", Arrays.asList(string, start, length));
    }

    /**
     * $add 表达式: { $add: [ <expression1>, <expression2>, ... ] }
     *
     * <p>对多个数值表达式执行加法运算。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算总分
     * Expressions.add("$math_score", "$english_score", "$science_score")
     * </pre>
     *
     * @param expressions 数值表达式数组
     *
     * @return 包含$add表达式的Document
     */
    public static Document add(Object... expressions) {
        return new Document("$add", Arrays.asList(expressions));
    }

    /**
     * $subtract 表达式: { $subtract: [ <expression1>, <expression2> ] }
     *
     * <p>对两个数值表达式执行减法运算。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算年龄差
     * Expressions.subtract("$age1", "$age2")
     * </pre>
     *
     * @param expr1 被减数表达式
     * @param expr2 减数表达式
     *
     * @return 包含$subtract表达式的Document
     */
    public static Document subtract(Object expr1, Object expr2) {
        return new Document("$subtract", Arrays.asList(expr1, expr2));
    }

    /**
     * $multiply 表达式: { $multiply: [ <expression1>, <expression2>, ... ] }
     *
     * <p>对多个数值表达式执行乘法运算。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算总价（单价*数量）
     * Expressions.multiply("$price", "$quantity")
     * </pre>
     *
     * @param expressions 数值表达式数组
     *
     * @return 包含$multiply表达式的Document
     */
    public static Document multiply(Object... expressions) {
        return new Document("$multiply", Arrays.asList(expressions));
    }

    /**
     * $divide 表达式: { $divide: [ <expression1>, <expression2> ] }
     *
     * <p>对两个数值表达式执行除法运算。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算平均分（总分/科目数）
     * Expressions.divide("$total_score", "$subject_count")
     * </pre>
     *
     * @param expr1 被除数表达式
     * @param expr2 除数表达式
     *
     * @return 包含$divide表达式的Document
     */
    public static Document divide(Object expr1, Object expr2) {
        return new Document("$divide", Arrays.asList(expr1, expr2));
    }

    /**
     * $abs 表达式: { $abs: <number> }
     *
     * <p>返回一个数字的绝对值。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取数值的绝对值
     * Expressions.abs("$temperature")
     * </pre>
     *
     * @param number 数字表达式
     *
     * @return 包含$abs表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/abs/">MongoDB $abs 文档</a>
     */
    public static Document abs(Object number) {
        return new Document("$abs", number);
    }

    /**
     * $ceil 表达式: { $ceil: <number> }
     *
     * <p>返回大于或等于指定数字的最小整数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 向上取整
     * Expressions.ceil("$price")
     * </pre>
     *
     * @param number 数字表达式
     *
     * @return 包含$ceil表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/ceil/">MongoDB $ceil 文档</a>
     */
    public static Document ceil(Object number) {
        return new Document("$ceil", number);
    }

    /**
     * $exp 表达式: { $exp: <exponent> }
     *
     * <p>将 e 提升到指定的指数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算 e 的幂
     * Expressions.exp("$power")
     * </pre>
     *
     * @param exponent 指数表达式
     *
     * @return 包含$exp表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/exp/">MongoDB $exp 文档</a>
     */
    public static Document exp(Object exponent) {
        return new Document("$exp", exponent);
    }

    /**
     * $floor 表达式: { $floor: <number> }
     *
     * <p>返回小于或等于指定数字的最大整数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 向下取整
     * Expressions.floor("$price")
     * </pre>
     *
     * @param number 数字表达式
     *
     * @return 包含$floor表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/floor/">MongoDB $floor 文档</a>
     */
    public static Document floor(Object number) {
        return new Document("$floor", number);
    }

    /**
     * $ln 表达式: { $ln: <number> }
     *
     * <p>计算数字的自然对数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算自然对数
     * Expressions.ln("$value")
     * </pre>
     *
     * @param number 数字表达式
     *
     * @return 包含$ln表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/ln/">MongoDB $ln 文档</a>
     */
    public static Document ln(Object number) {
        return new Document("$ln", number);
    }

    /**
     * $log 表达式: { $log: [ <number>, <base> ] }
     *
     * <p>以指定基数计算数字的对数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算以10为底的对数
     * Expressions.log("$value", 10)
     * </pre>
     *
     * @param number 数字表达式
     * @param base   基数
     *
     * @return 包含$log表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/log/">MongoDB $log 文档</a>
     */
    public static Document log(Object number, Object base) {
        return new Document("$log", Arrays.asList(number, base));
    }

    /**
     * $log10 表达式: { $log10: <number> }
     *
     * <p>计算一个数字以 10 为底的对数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算以10为底的对数
     * Expressions.log10("$value")
     * </pre>
     *
     * @param number 数字表达式
     *
     * @return 包含$log10表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/log10/">MongoDB $log10 文档</a>
     */
    public static Document log10(Object number) {
        return new Document("$log10", number);
    }

    /**
     * $mod 表达式: { $mod: [ <dividend>, <divisor> ] }
     *
     * <p>返回第一个数字除以第二个数字的余数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算除法余数
     * Expressions.mod("$dividend", "$divisor")
     * </pre>
     *
     * @param dividend 被除数表达式
     * @param divisor  除数表达式
     *
     * @return 包含$mod表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/mod/">MongoDB $mod 文档</a>
     */
    public static Document mod(Object dividend, Object divisor) {
        return new Document("$mod", Arrays.asList(dividend, divisor));
    }

    /**
     * $pow 表达式: { $pow: [ <number>, <exponent> ] }
     *
     * <p>将一个数字提升到指定的指数。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算幂运算
     * Expressions.pow("$base", "$exponent")
     * </pre>
     *
     * @param number   底数表达式
     * @param exponent 指数表达式
     *
     * @return 包含$pow表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/pow/">MongoDB $pow 文档</a>
     */
    public static Document pow(Object number, Object exponent) {
        return new Document("$pow", Arrays.asList(number, exponent));
    }

    /**
     * $round 表达式: { $round: [ <number>, <place> ] }
     *
     * <p>将数字舍入到整数或指定的小数位。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 四舍五入到整数
     * Expressions.round("$value")
     *
     * // 四舍五入到两位小数
     * Expressions.round("$value", 2)
     * </pre>
     *
     * @param number 数字表达式
     * @param place  小数位数（可选）
     *
     * @return 包含$round表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/round/">MongoDB $round 文档</a>
     */
    public static Document round(Object number, Object... place) {
        if (place.length == 0) {
            return new Document("$round", number);
        } else {
            return new Document("$round", Arrays.asList(number, place[0]));
        }
    }

    /**
     * $sqrt 表达式: { $sqrt: <number> }
     *
     * <p>计算平方根。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算平方根
     * Expressions.sqrt("$value")
     * </pre>
     *
     * @param number 数字表达式
     *
     * @return 包含$sqrt表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/sqrt/">MongoDB $sqrt 文档</a>
     */
    public static Document sqrt(Object number) {
        return new Document("$sqrt", number);
    }

    /**
     * $trunc 表达式: { $trunc: [ <number>, <place> ] }
     *
     * <p>将数字截断为整数或指定的小数位。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 截断到整数
     * Expressions.trunc("$value")
     *
     * // 截断到两位小数
     * Expressions.trunc("$value", 2)
     * </pre>
     *
     * @param number 数字表达式
     * @param place  小数位数（可选）
     *
     * @return 包含$trunc表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/trunc/">MongoDB $trunc 文档</a>
     */
    public static Document trunc(Object number, Object... place) {
        if (place.length == 0) {
            return new Document("$trunc", number);
        } else {
            return new Document("$trunc", Arrays.asList(number, place[0]));
        }
    }

    /**
     * $arrayElemAt 表达式: { $arrayElemAt: [ <array>, <idx> ] }
     *
     * <p>返回位于指定数组索引处的元素。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取数组的第一个元素
     * Expressions.arrayElemAt("$tags", 0)
     * </pre>
     *
     * @param array 数组表达式
     * @param idx   索引
     *
     * @return 包含$arrayElemAt表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/arrayElemAt/">MongoDB $arrayElemAt 文档</a>
     */
    public static Document arrayElemAt(Object array, Object idx) {
        return new Document("$arrayElemAt", Arrays.asList(array, idx));
    }

    /**
     * $concatArrays 表达式: { $concatArrays: [ <array1>, <array2>, ... ] }
     *
     * <p>连接数组以返回连接后的数组。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 连接两个数组
     * Expressions.concatArrays("$array1", "$array2")
     * </pre>
     *
     * @param arrays 数组表达式数组
     *
     * @return 包含$concatArrays表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/concatArrays/">MongoDB $concatArrays 文档</a>
     */
    public static Document concatArrays(Object... arrays) {
        return new Document("$concatArrays", Arrays.asList(arrays));
    }

    /**
     * $filter 表达式: { $filter: { input: <array>, cond: <condition>, as: <string>, limit: <number> } }
     *
     * <p>选择数组的子集，以返回仅包含与筛选条件匹配的元素的数组。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 过滤数组中大于10的元素
     * Document filterCond = Expressions.gt("$$item", 10);
     * Expressions.filter("$items", filterCond, "item")
     * </pre>
     *
     * @param input 输入数组表达式
     * @param cond  条件表达式
     * @param as    变量名（可选）
     *
     * @return 包含$filter表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/filter/">MongoDB $filter 文档</a>
     */
    public static Document filter(Object input, Object cond, String... as) {
        Document filterDoc = new Document("input", input).append("cond", cond);
        if (as.length > 0) {
            filterDoc.append("as", as[0]);
        }
        return new Document("$filter", filterDoc);
    }

    /**
     * $map 表达式: { $map: { input: <expression>, as: <string>, in: <expression> } }
     *
     * <p>对数组的每个元素应用子表达式，并按顺序返回生成值的数组。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 对数组中每个元素乘以2
     * Document inExpr = Expressions.multiply("$$num", 2);
     * Expressions.map("$numbers", "num", inExpr)
     * </pre>
     *
     * @param input 输入数组表达式
     * @param as    变量名
     * @param in    输入表达式
     *
     * @return 包含$map表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/map/">MongoDB $map 文档</a>
     */
    public static Document map(Object input, String as, Object in) {
        return new Document("$map", new Document("input", input)
                .append("as", as)
                .append("in", in));
    }

    /**
     * $range 表达式: { $range: [ <start>, <end>, <non-zero step> ] }
     *
     * <p>根据用户定义的输入，输出一个包含整数序列的数组。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 生成0到10的数组
     * Expressions.range(0, 10)
     *
     * // 生成0到10，步长为2的数组
     * Expressions.range(0, 10, 2)
     * </pre>
     *
     * @param start 起始值
     * @param end   结束值
     * @param step  步长（可选，默认为1）
     *
     * @return 包含$range表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/range/">MongoDB $range 文档</a>
     */
    public static Document range(Object start, Object end, Object... step) {
        if (step.length == 0) {
            return new Document("$range", Arrays.asList(start, end));
        } else {
            return new Document("$range", Arrays.asList(start, end, step[0]));
        }
    }

    /**
     * $reduce 表达式: { $reduce: { input: <array>, initialValue: <expression>, in: <expression> } }
     *
     * <p>将表达式应用于数组中的每个元素，并将它们组合成一个值。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算数组元素总和
     * Document inExpr = Expressions.add("$$value", "$$this");
     * Expressions.reduce("$numbers", 0, inExpr)
     * </pre>
     *
     * @param input        输入数组表达式
     * @param initialValue 初始值
     * @param in           输入表达式
     *
     * @return 包含$reduce表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/reduce/">MongoDB $reduce 文档</a>
     */
    public static Document reduce(Object input, Object initialValue, Object in) {
        return new Document("$reduce", new Document("input", input)
                .append("initialValue", initialValue)
                .append("in", in));
    }

    /**
     * $reverseArray 表达式: { $reverseArray: <array> }
     *
     * <p>返回元素顺序相反的数组。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 反转数组
     * Expressions.reverseArray("$tags")
     * </pre>
     *
     * @param array 数组表达式
     *
     * @return 包含$reverseArray表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/reverseArray/">MongoDB $reverseArray 文档</a>
     */
    public static Document reverseArray(Object array) {
        return new Document("$reverseArray", array);
    }

    /**
     * $slice 表达式: { $slice: [ <array>, <n> ] } 或 { $slice: [ <array>, <position>, <n> ] }
     *
     * <p>返回数组的子集。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取数组前5个元素
     * Expressions.slice("$items", 5)
     *
     * // 从位置2开始获取3个元素
     * Expressions.slice("$items", 2, 3)
     * </pre>
     *
     * @param array 数组表达式
     * @param n     元素数量或起始位置
     * @param count 元素数量（可选）
     *
     * @return 包含$slice表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/slice/">MongoDB $slice 文档</a>
     */
    public static Document slice(Object array, Object n, Object... count) {
        if (count.length == 0) {
            return new Document("$slice", Arrays.asList(array, n));
        } else {
            return new Document("$slice", Arrays.asList(array, n, count[0]));
        }
    }

    /**
     * $zip 表达式: { $zip: { inputs: [ <array1>, <array2>, ... ], useLongestLength: <boolean>, defaults: <array> } }
     *
     * <p>将两个数组进行合并。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 合并两个数组
     * Expressions.zip(Arrays.asList("$array1", "$array2"))
     * </pre>
     *
     * @param inputs 输入数组列表
     *
     * @return 包含$zip表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/zip/">MongoDB $zip 文档</a>
     */
    public static Document zip(Object inputs) {
        return new Document("$zip", new Document("inputs", inputs));
    }

    /**
     * $cmp 表达式: { $cmp: [ <expression1>, <expression2> ] }
     *
     * <p>如果两个值相等，则返回 0；如果第一个值大于第二个值，则返回 1；如果第一个值小于第二个值，则返回 -1。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 比较两个值
     * Expressions.cmp("$value1", "$value2")
     * </pre>
     *
     * @param expr1 第一个表达式
     * @param expr2 第二个表达式
     *
     * @return 包含$cmp表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/cmp/">MongoDB $cmp 文档</a>
     */
    public static Document cmp(Object expr1, Object expr2) {
        return new Document("$cmp", Arrays.asList(expr1, expr2));
    }

    /**
     * $switch 表达式: { $switch: { branches: [ { case: <expression>, then: <expression> }, ... ], default: <expression> } }
     *
     * <p>对一系列 case 表达式求值。当它找到计算结果为 true 的表达式时，$switch 会执行指定表达式并脱离控制流。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 多条件分支
     * List<Document> branches = Arrays.asList(
     *     new Document("case", Expressions.eq("$score", 100)).append("then", "Perfect"),
     *     new Document("case", Expressions.gte("$score", 90)).append("then", "Excellent")
     * );
     * Expressions.switchCase(branches, "Good")
     * </pre>
     *
     * @param branches    分支列表
     * @param defaultCase 默认值
     *
     * @return 包含$switch表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/switch/">MongoDB $switch 文档</a>
     */
    public static Document switchCase(Object branches, Object defaultCase) {
        return new Document("$switch", new Document("branches", branches)
                .append("default", defaultCase));
    }

    /**
     * $dateAdd 表达式: { $dateAdd: { startDate: <date>, unit: <unit>, amount: <number>, timezone: <tzExpression> } }
     *
     * <p>向日期对象添加多个时间单位。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 向日期添加1天
     * Expressions.dateAdd("$create_time", "day", 1)
     * </pre>
     *
     * @param startDate 起始日期
     * @param unit      时间单位
     * @param amount    数量
     * @param timezone  时区（可选）
     *
     * @return 包含$dateAdd表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/dateAdd/">MongoDB $dateAdd 文档</a>
     */
    public static Document dateAdd(Object startDate, String unit, Object amount, String... timezone) {
        Document dateAddDoc = new Document("startDate", startDate)
                .append("unit", unit)
                .append("amount", amount);
        if (timezone.length > 0) {
            dateAddDoc.append("timezone", timezone[0]);
        }
        return new Document("$dateAdd", dateAddDoc);
    }

    /**
     * $dateDiff 表达式: { $dateDiff: { startDate: <date>, endDate: <date>, unit: <unit>, timezone: <tzExpression>, startOfWeek: <string> } }
     *
     * <p>返回两个日期之间的差值。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 计算两个日期之间的天数差
     * Expressions.dateDiff("$start_date", "$end_date", "day")
     * </pre>
     *
     * @param startDate 起始日期
     * @param endDate   结束日期
     * @param unit      时间单位
     * @param timezone  时区（可选）
     *
     * @return 包含$dateDiff表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/dateDiff/">MongoDB $dateDiff 文档</a>
     */
    public static Document dateDiff(Object startDate, Object endDate, String unit, String... timezone) {
        Document dateDiffDoc = new Document("startDate", startDate)
                .append("endDate", endDate)
                .append("unit", unit);
        if (timezone.length > 0) {
            dateDiffDoc.append("timezone", timezone[0]);
        }
        return new Document("$dateDiff", dateDiffDoc);
    }

    /**
     * $dayOfMonth 表达式: { $dayOfMonth: <dateExpression> }
     *
     * <p>以介于 1 和 31 之间的数字返回某一日期的"月中的某一天"。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的天数
     * Expressions.dayOfMonth("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$dayOfMonth表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/dayOfMonth/">MongoDB $dayOfMonth 文档</a>
     */
    public static Document dayOfMonth(Object dateExpression) {
        return new Document("$dayOfMonth", dateExpression);
    }

    /**
     * $dayOfWeek 表达式: { $dayOfWeek: <dateExpression> }
     *
     * <p>以 1（星期日）和 7（星期六）之间的数字形式返回以星期表示的日期。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的星期几
     * Expressions.dayOfWeek("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$dayOfWeek表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/dayOfWeek/">MongoDB $dayOfWeek 文档</a>
     */
    public static Document dayOfWeek(Object dateExpression) {
        return new Document("$dayOfWeek", dateExpression);
    }

    /**
     * $dayOfYear 表达式: { $dayOfYear: <dateExpression> }
     *
     * <p>以 1 到 366（闰年）之间的数字形式返回返回日期的年月日。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的年中天数
     * Expressions.dayOfYear("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$dayOfYear表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/dayOfYear/">MongoDB $dayOfYear 文档</a>
     */
    public static Document dayOfYear(Object dateExpression) {
        return new Document("$dayOfYear", dateExpression);
    }

    /**
     * $hour 表达式: { $hour: <dateExpression> }
     *
     * <p>以数字形式返回日期中的小时部分（0 到 23）。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的小时
     * Expressions.hour("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$hour表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/hour/">MongoDB $hour 文档</a>
     */
    public static Document hour(Object dateExpression) {
        return new Document("$hour", dateExpression);
    }

    /**
     * $minute 表达式: { $minute: <dateExpression> }
     *
     * <p>返回日期的分钟数（0 到 59）。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的分钟
     * Expressions.minute("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$minute表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/minute/">MongoDB $minute 文档</a>
     */
    public static Document minute(Object dateExpression) {
        return new Document("$minute", dateExpression);
    }

    /**
     * $month 表达式: { $month: <dateExpression> }
     *
     * <p>以 1（一月）到 12（十二月）之间的数字形式返回日期的月份。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的月份
     * Expressions.month("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$month表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/month/">MongoDB $month 文档</a>
     */
    public static Document month(Object dateExpression) {
        return new Document("$month", dateExpression);
    }

    /**
     * $second 表达式: { $second: <dateExpression> }
     *
     * <p>以 0 到 60 之间的数字返回日期的秒数（跳秒）。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的秒数
     * Expressions.second("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$second表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/second/">MongoDB $second 文档</a>
     */
    public static Document second(Object dateExpression) {
        return new Document("$second", dateExpression);
    }

    /**
     * $year 表达式: { $year: <dateExpression> }
     *
     * <p>返回日期的年份。</p>
     *
     * <p>使用示例：</p>
     * <pre>
     * // 获取日期中的年份
     * Expressions.year("$create_time")
     * </pre>
     *
     * @param dateExpression 日期表达式
     *
     * @return 包含$year表达式的Document
     *
     * @see <a href="https://www.mongodb.com/zh-cn/docs/manual/reference/operator/aggregation/year/">MongoDB $year 文档</a>
     */
    public static Document year(Object dateExpression) {
        return new Document("$year", dateExpression);
    }

}
