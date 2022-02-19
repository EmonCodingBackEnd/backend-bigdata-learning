package com.coding.bigdata.flume;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {

    private List<Event> events;

    // 初始化
    @Override
    public void initialize() {
        events = new ArrayList<>();
    }

    // 单个事件
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        JSONObject jsonObj = JSONObject.parseObject(body);

        String timestamp;
        Long acttime;
        if (jsonObj.containsKey("acttime")) {
            acttime = jsonObj.getLong("acttime");
        } else {
            acttime = System.currentTimeMillis();
        }
        timestamp = String.valueOf(acttime);

        headers.put("timestamp", timestamp);
        return event;
    }

    // 多个事件
    @Override
    public List<Event> intercept(List<Event> list) {
        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }
        return events;
    }

    @Override
    public void close() {
        events = null;
    }

    // flume自定义拦截器规范
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {}
    }
}
