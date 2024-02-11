package io.github.chaogeoop.base.business.common.helpers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class SleepHelper {
    public static void sleep(Duration duration) {
        try {
            long millis = duration.toMillis();

            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
