package com.coding.bigdata.useraction;

import com.alibaba.fastjson.JSONObject;
import com.coding.bigdata.useraction.utils.HttpUtils;
import com.coding.bigdata.useraction.utils.MyDBUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

/**
 * 需求：生成商品订单相关数据（服务端数据），初始化到MySQL中
 *
 * <p>【注意】：在执行代码之前需要先执行init_mysql_tables.sql脚本进行数据库和表的初始化.
 *
 * <p>【服务端数据】商品订单相关数据（用户表、商品表、订单表等）
 */
@Slf4j
public class GenerateGoodsOrderData {

    public static void main(String[] args) {
        // 通过接口获取用户行为数据
        String dataUrl = "http://data.xuwei.tech/d1/go1";
        JSONObject paramObj = new JSONObject();
        // TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code", "DC68F89C4E9D0416"); // 校验码
        paramObj.put("date", "2026-01-01"); // 指定数据产生的日期
        paramObj.put("user_num", 100); // 指定生成的用户数量
        paramObj.put("order_num", 1000); // 指定生成的订单数量
        // insert into t1(...) values(...)
        JSONObject dataObj = HttpUtils.doPost(dataUrl, paramObj);

        // 判断获取的用户行为数据是否正确
        boolean flag = dataObj.containsKey("error");
        if (!flag) {
            // 通过JDBC的方式初始化数据到MySQL中
            String data = dataObj.getString("data");
            String[] splits = data.split("\n");
            long start = System.currentTimeMillis();
            log.info("===============start init===============");
            ArrayList<String> tmpSqlList = new ArrayList<String>();
            for (int i = 0; i < splits.length; i++) {
                tmpSqlList.add(splits[i]);
                if (tmpSqlList.size() % 100 == 0) {
                    MyDBUtils.batchUpdate(tmpSqlList);
                    tmpSqlList.clear();
                }
            }
            // 把剩余的数据批量添加到数据库中
            MyDBUtils.batchUpdate(tmpSqlList);
            log.info("===============end init==============");
            long end = System.currentTimeMillis();
            log.info("===============耗时: " + (end - start) / 1000 + "秒===============");
        } else {
            log.error("获取商品订单相关数据错误：" + dataObj.toJSONString());
        }
    }
}
