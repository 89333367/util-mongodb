package sunyu.util.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 自定义列注解，用于标识实体类字段与数据库列的映射关系
 */
@Target(ElementType.FIELD)  // 注解作用于字段上
@Retention(RetentionPolicy.RUNTIME)  // 运行时保留，方便通过反射获取
public @interface Column {

    /**
     * 数据库列名
     */
    String column();

    /**
     * 列描述信息
     */
    String desc() default "";

}