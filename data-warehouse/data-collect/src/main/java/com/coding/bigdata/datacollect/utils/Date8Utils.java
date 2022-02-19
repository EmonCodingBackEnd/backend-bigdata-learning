package com.coding.bigdata.datacollect.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.*;
import java.util.Date;

@Slf4j
public abstract class Date8Utils {

    public static Date asDate(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Date asDate(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDate asLocalDate(Date date) {
        if (date == null) {
            return null;
        }
        return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public static LocalDateTime asLocalDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return Instant.ofEpochMilli(date.getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
    }

    public static LocalDateTime longSecond2LocalDateTime(long secondTime) {
        return Instant.ofEpochSecond(secondTime)
                .atOffset(ZoneOffset.of("+08:00"))
                .toLocalDateTime();
    }

    public static LocalDateTime longMilli2LocalDateTime(long milliTime) {
        return Instant.ofEpochMilli(milliTime).atOffset(ZoneOffset.of("+08:00")).toLocalDateTime();
    }
}
