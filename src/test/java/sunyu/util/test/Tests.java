package sunyu.util.test;

import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class Tests {
    Log log = LogFactory.get();

    static List<String> dateFormats = Arrays.asList(
            "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SS", "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss",
            "yyyy-M-d HH:mm:ss", "yyyy/M/d HH:mm:ss",
            "yyyy-MM-dd", "yyyy/MM/dd",
            "yyyy-M-d", "yyyy/M/d"
    );

    @Test
    void testParseLocalDateTime() {
        for (String dateFormat : dateFormats) {
            try {
                LocalDateTime ldt = LocalDateTimeUtil.parse("2021-11-25 00:00:00.0", dateFormat);
                log.debug("ldt: {}", ldt);
            } catch (Exception e) {
            }
        }
    }
}
