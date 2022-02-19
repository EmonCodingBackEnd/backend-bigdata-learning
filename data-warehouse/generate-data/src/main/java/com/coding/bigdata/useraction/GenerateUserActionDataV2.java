package com.coding.bigdata.useraction;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.coding.bigdata.useraction.utils.GenerateDateAndActArrUtils;
import com.coding.bigdata.useraction.utils.HDFSOpUtils;
import com.coding.bigdata.useraction.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 需求：生成用户行为数据(客户端数据) 生成2026-02-01~2026-02-28的数据 Created by xuwei */
public class GenerateUserActionDataV2 {
    private static final Logger logger = LoggerFactory.getLogger(GenerateUserActionDataV2.class);

    public static void main(String[] args) throws Exception {
        // 通过接口获取用户行为数据
        String dataUrl = "http://data.xuwei.tech/d1/wh2";
        JSONObject paramObj = new JSONObject();
        // TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code", "DC68F89C4E9D0416"); // 校验码
        JSONObject dataObj = HttpUtils.doPost(dataUrl, paramObj);
        // logger.info(dataObj.toJSONString());
        // 判断获取的用户行为数据是否正确
        boolean flag = dataObj.containsKey("error");
        if (!flag) {
            long start = System.currentTimeMillis();
            logger.info("===============start 上传数据==============");
            // 从dataObj中获取每一天，每一种类型的数据
            String[] dateArr = GenerateDateAndActArrUtils.getDateArr();
            int[] actArr = GenerateDateAndActArrUtils.getActArr();
            for (int i = 0; i < dateArr.length; i++) {
                String dt = dateArr[i];
                for (int j = 0; j < actArr.length; j++) {
                    int act = actArr[j];
                    // 获取某一天某一类型的数据
                    JSONArray resArr = dataObj.getJSONArray(dt + "_" + act);
                    StringBuffer sb = new StringBuffer();
                    for (int m = 0; m < resArr.size(); m++) {
                        JSONObject jsonObj = resArr.getJSONObject(m);
                        String line = jsonObj.toJSONString();
                        if (m == 0) {
                            sb.append(line);
                        } else {
                            sb.append("\n" + line);
                        }
                    }
                    // 将数据上传到HDFS上面，注意：需要关闭HDFS的权限校验机制
                    String hdfsOutPath =
                            "hdfs://emon:8020/custom/data/warehouse/ods/user_action/"
                                    + dt.replace("-", "")
                                    + "/"
                                    + act;
                    String fileName = dt + "-" + act + ".log";
                    logger.info("开始上传：" + hdfsOutPath + "/" + fileName);
                    HDFSOpUtils.put(sb.toString(), hdfsOutPath, fileName);
                }
            }
            logger.info("===============end 上传数据==============");
            long end = System.currentTimeMillis();
            logger.info("===============耗时: " + (end - start) / 1000 + "秒===============");
        } else {
            logger.error("上传用户行为数据失败：" + dataObj.toJSONString());
        }
    }
}
