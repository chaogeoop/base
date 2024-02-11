package io.github.chaogeoop.base.business.common.helpers;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

public class DateHelper {
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();

    public static Date parseStringDate(String stringDate, DateFormatEnum format) {
        if (format.getIsTime()) {
            LocalDateTime dateTime = LocalDateTime.parse(stringDate, format.getFormat());
            Instant instant = dateTime.atZone(DEFAULT_ZONE_ID).toInstant();
            return Date.from(instant);
        }

        LocalDate localDate = LocalDate.parse(stringDate, format.getFormat());
        Instant instant = localDate.atStartOfDay(DEFAULT_ZONE_ID).toInstant();
        return Date.from(instant);
    }

    public static String dateToString(Date date, DateFormatEnum format) {
        LocalDateTime localDateTime = Instant.ofEpochMilli(date.getTime()).atZone(DEFAULT_ZONE_ID).toLocalDateTime();
        return localDateTime.format(format.getFormat());
    }

    public static Date startOfDay(Date date) {
        return startOfDay(date, 0);
    }

    public static Date startOfDay(Date date, int adjustDays) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate()
                .plusDays(adjustDays)
                .atStartOfDay(DEFAULT_ZONE_ID)
                .toInstant();
        return Date.from(instant);
    }

    public static Date endOfDay(Date date) {
        return endOfDay(date, 0);
    }

    public static Date endOfDay(Date date, int adjustDays) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate()
                .atStartOfDay(DEFAULT_ZONE_ID)
                .plusDays(1)
                .plusSeconds(-1)
                .plusDays(adjustDays)
                .toInstant();
        return Date.from(instant);
    }

    public static Date startOfMonth(Date date) {
        return startOfMonth(date, 0);
    }

    public static Date startOfMonth(Date date, int adjustMonths) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate()
                .atStartOfDay(DEFAULT_ZONE_ID)
                .with(TemporalAdjusters.firstDayOfMonth())
                .plusMonths(adjustMonths)
                .toInstant();
        return Date.from(instant);
    }

    public static Date endOfMonth(Date date) {
        return endOfMonth(date, 0);
    }

    public static Date endOfMonth(Date date, int adjustMonths) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate()
                .atStartOfDay(DEFAULT_ZONE_ID)
                .with(TemporalAdjusters.lastDayOfMonth())
                .plusMonths(adjustMonths)
                .plusDays(1)
                .minusSeconds(1)
                .toInstant();
        return Date.from(instant);
    }

    public static Date startOfYear(Date date) {
        return startOfYear(date, 0);
    }

    public static Date startOfYear(Date date, int adjustYear) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate()
                .atStartOfDay(DEFAULT_ZONE_ID)
                .with(TemporalAdjusters.firstDayOfYear())
                .plusYears(adjustYear)
                .toInstant();
        return Date.from(instant);
    }

    public static Date endOfYear(Date date) {
        return endOfYear(date, 0);
    }

    public static Date endOfYear(Date date, int adjustYear) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate()
                .atStartOfDay(DEFAULT_ZONE_ID)
                .with(TemporalAdjusters.lastDayOfYear())
                .plusYears(adjustYear)
                .plusDays(1)
                .minusSeconds(1)
                .toInstant();
        return Date.from(instant);
    }

    public static LocalDate toLocalDate(Date date) {
        return Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDate();
    }

    public static LocalDateTime toLocalDateTime(Date date) {
        return Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .toLocalDateTime();
    }

    public static int diffDays(Date referDate, Date date) {
        long days = ChronoUnit.DAYS.between(toLocalDate(referDate), toLocalDate(date));
        return (int) days;
    }

    public static int diffSeconds(Date referDate, Date date) {
        long seconds = ChronoUnit.SECONDS.between(toLocalDateTime(referDate), toLocalDateTime(date));
        return (int) seconds;
    }

    public static int diffMonths(Date referDate, Date date) {
        long years = ChronoUnit.MONTHS.between(toLocalDate(referDate), toLocalDate(date));
        return (int) years;
    }

    public static int diffYears(Date referDate, Date date) {
        long years = ChronoUnit.YEARS.between(toLocalDate(referDate), toLocalDate(date));
        return (int) years;
    }

    public static Date plusDurationOfDate(Date date, Duration duration) {
        Instant instant = Instant.ofEpochMilli(date.getTime())
                .atZone(DEFAULT_ZONE_ID)
                .plus(duration)
                .toInstant();
        return Date.from(instant);
    }

    public enum DateFormatEnum {
        fullUntilDay(DateTimeFormatter.ofPattern("yyyy-MM-dd"), false),
        fullUntilMonth(DateTimeFormatter.ofPattern("yyyy-MM"), false),
        fullUntilSecond(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"), true),
        fullUntilMill(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"), true),
        isoFormat(DateTimeFormatter.ISO_LOCAL_DATE_TIME, true),
        onlyTimeUntilSecond(DateTimeFormatter.ofPattern("HH:mm:ss"), true),
        onlyTimeUntilMill(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"), true),
        ;

        private final DateTimeFormatter format;
        private final boolean isTime;

        public DateTimeFormatter getFormat() {
            return this.format;
        }

        public boolean getIsTime() {
            return this.isTime;
        }

        DateFormatEnum(DateTimeFormatter format, boolean isTime) {
            this.format = format;
            this.isTime = isTime;
        }
    }
}
