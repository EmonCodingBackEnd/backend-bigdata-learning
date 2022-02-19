package com.coding.bigdata.useraction;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.coding.bigdata.useraction.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 需求：生成用户行为数据(客户端数据)，模拟埋点上报数据.
 *
 * <p>【客户端数据】用户行为数据（打开APP、点击、浏览等）
 */
public class GenerateUserActionData {
    private static final Logger logger = LoggerFactory.getLogger(GenerateUserActionData.class);

    public static void main(String[] args) {
        // 通过接口获取用户行为数据
        String dataUrl = "http://data.xuwei.tech/d1/wh1";
//        dataUrl = "http://emon:8080/d1/wh1";
        JSONObject paramObj = new JSONObject();
        // TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code", "DC68F89C4E9D0416"); // 校验码
        paramObj.put("num", 100); // 指定生成多少用户的行为数据
        paramObj.put("date", "2026-01-01"); // 指定用户行为产生的日期
        // {"data":[{"uid":"100001","ver":"3.0.0","data":"[{\"act\":4,\"loading_time\":823,\"goods_num\":9,\"acttime\":1577865686000,\"loading_type\":1},{\"act\":3,\"loading_time\":482,\"goods_id\":\"100090\",\"acttime\":1577865686000,\"stay_time\":4284},{\"act\":1,\"loading_time\":501,\"acttime\":1577865686000,\"ad_status\":1},{\"act\":3,\"loading_time\":598,\"goods_id\":\"100033\",\"acttime\":1577865686000,\"stay_time\":4537}]","display":"1334x750","model":"huawei21","net":5,"xaid":"ab25617-c38910-m1","brand":"huawei","osver":"5.1","vercode":"35100053","platform":2}]}
        JSONObject dataObj = HttpUtils.doPost(dataUrl, paramObj);
        logger.info(dataObj.toJSONString());
        // 判断获取的用户行为数据是否正确
//        boolean flag = dataObj.getBoolean("success");
        boolean flag = dataObj.containsKey("error");
        if (!flag) {
            // TODO 调用接口模拟埋点上报数据，这里的主机名需要改为实际机器的主机名(ip)
            String uploadUrl = "http://emon:8080/v1/ua";
            // 获取dataObj中的data属性的值
            JSONArray resArr = dataObj.getJSONArray("data");
            long start = System.currentTimeMillis();
            logger.info("===============start 上报数据==============");
            // 迭代接口返回的数据，一条一条模拟上报
            for (int i = 0; i < resArr.size(); i++) {
                JSONObject resObj = resArr.getJSONObject(i);
                JSONObject res = HttpUtils.doPost(uploadUrl, resObj);
                int status = res.getIntValue("status");
                if (status != 200) {
                    logger.error("数据上报失败：" + resObj.toJSONString());
                } else {
                    logger.info("数据上报成功：第 " + (i + 1) + " 条！");
                }
            }
            logger.info("===============end 上报数据==============");
            long end = System.currentTimeMillis();
            logger.info("===============耗时: " + (end - start) / 1000 + "秒===============");
        } else {
            logger.error("获取用户行为数据失败：" + dataObj.toJSONString());
        }
    }
}
