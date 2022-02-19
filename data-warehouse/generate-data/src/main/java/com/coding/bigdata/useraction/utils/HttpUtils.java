package com.coding.bigdata.useraction.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/*
 * HTTP请求工具类
 */
public class HttpUtils {

    public static JSONObject doPost(String url, JSONObject jsonObj) {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);
        JSONObject response = null;

        try {
            StringEntity stringEntity = new StringEntity(jsonObj.toString());
            stringEntity.setContentEncoding("UTF-8");
            // 发送json数据需要设置contentType
            stringEntity.setContentType("application/json");
            post.setEntity(stringEntity);
            CloseableHttpResponse res = client.execute(post);
            if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = res.getEntity();
                // 返回json格式
                String result = EntityUtils.toString(entity);
                response = JSON.parseObject(result);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
        }
        return response;
    }

    public static void main(String[] args) {
        String url = "http://localhost:8080/v1/ua";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "tom");
        jsonObject.put("age", 18);
        JSONObject res = doPost(url, jsonObject);
        System.out.println(res.toString());
    }
}
