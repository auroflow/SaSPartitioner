package cn.edu.zju.daily.metricflux.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class TimeUtils {

    public static List<Long> ratesToDelays(List<Long> rates) {
        List<Long> results = new ArrayList<>();
        for (long rate : rates) {
            results.add(rateToDelay(rate));
        }
        return results;
    }

    public static long rateToDelay(long rate) {
        if (rate > 0) {
            // rate > 0 means events per second
            return 1_000_000_000L / rate;
        } else if (rate == 0) {
            // rate == 0 means no limit
            return 0;
        } else {
            // rate < 0 means seconds per event
            return -rate * 1_000_000_000L;
        }
    }

    public static String unixTimestampToString(long milli) {
        return localDateTimeToString(
                LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(milli), TimeZone.getDefault().toZoneId()));
    }

    public static String localDateTimeToString(LocalDateTime time) {
        return DateTimeFormatter.ISO_DATE_TIME.format(time);
    }
}
