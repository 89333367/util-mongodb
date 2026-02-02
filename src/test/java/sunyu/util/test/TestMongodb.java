package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sunyu.util.MongoDBUtil;
import sunyu.util.test.config.ConfigProperties;

public class TestMongodb {
    Log log = LogFactory.get();
    static Props props = ConfigProperties.getProps();
    static MongoDBUtil mongoDBUtil;

    @BeforeAll
    public static void init() {
        mongoDBUtil = MongoDBUtil.builder()
                // MongoDB连接字符串，包含用户名、密码、主机地址和连接参数
                .setUri(props.getStr("mongodb.uri"))
                .build();
    }

    @AfterAll
    public static void destroy() {
        mongoDBUtil.close();
    }

    @Test
    void hello() {
        MongoDatabase testdb = mongoDBUtil.getDatabase("testdb");
        log.info("{}", testdb);
    }

}
