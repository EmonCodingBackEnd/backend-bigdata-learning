package com.coding.bigdata.useraction;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.coding.bigdata.useraction.utils.GenerateDateAndActArrUtils;
import com.coding.bigdata.useraction.utils.HDFSOpUtils;
import com.coding.bigdata.useraction.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 需求：生成订单表拉链基础数据 生成2026-03-01~2026-03-03的数据 Created by xuwei */
public class GenerateZipData {
    private static final Logger logger = LoggerFactory.getLogger(GenerateZipData.class);

    public static void main(String[] args) throws Exception {
        // 通过接口获取用户行为数据
        String dataUrl = "http://data.xuwei.tech/d1/go3";
        JSONObject paramObj = new JSONObject();
        // TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code", "imooc"); // 校验码
        JSONObject dataObj = HttpUtils.doPost(dataUrl, paramObj);
        // logger.info(dataObj.toJSONString());
        // 判断获取的用户行为数据是否正确
        boolean flag = dataObj.containsKey("error");
        if (!flag) {
            long start = System.currentTimeMillis();
            logger.info("===============start 上传数据==============");
            // 从dataObj中获取每一天，每一种类型的数据
            String[] dateArr = GenerateDateAndActArrUtils.getZipDateArr();
            for (int i = 0; i < dateArr.length; i++) {
                String dt = dateArr[i];
                String tableName = "user_order";
                // 获取某一天某一类型的数据
                JSONArray resArr = dataObj.getJSONArray(tableName + "_" + dt);
                StringBuffer sb = new StringBuffer();
                for (int m = 0; m < resArr.size(); m++) {
                    JSONObject jsonObj = resArr.getJSONObject(m);
                    String line = jsonObj.getString("line");
                    if (m == 0) {
                        sb.append(line);
                    } else {
                        sb.append("\n" + line);
                    }
                }
                // 将数据上传到HDFS上面，注意：需要关闭HDFS的权限校验机制
                String hdfsOutPath =
                        "hdfs://bigdata01:9000/data/ods/" + tableName + "/" + dt.replace("-", "");
                String fileName = tableName + "-" + dt + ".log";
                logger.info("开始上传：" + hdfsOutPath + "/" + fileName);
                HDFSOpUtils.put(sb.toString(), hdfsOutPath, fileName);
            }
            logger.info("===============end 上传数据==============");
            long end = System.currentTimeMillis();
            logger.info("===============耗时: " + (end - start) / 1000 + "秒===============");
        } else {
            logger.error("上传用户行为数据失败：" + dataObj.toJSONString());
        }
    }
}
