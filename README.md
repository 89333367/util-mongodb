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
    <!-- {mongodb.driver.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <version>5.6.2_2.0_jdk8_x64</version>
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

```

