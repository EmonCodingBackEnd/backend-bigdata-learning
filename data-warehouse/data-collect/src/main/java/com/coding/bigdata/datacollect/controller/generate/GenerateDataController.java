package com.coding.bigdata.datacollect.controller.generate;

import com.alibaba.fastjson.JSONObject;
import com.coding.bigdata.datacollect.bean.generate.*;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@RestController
public class GenerateDataController {

    @PostMapping("/d1/wh1")
    public D1wh1Response d1wh1(@RequestBody(required = false) JSONObject jsonObj) {
        D1wh1Response response = new D1wh1Response();

        Integer num = jsonObj.getInteger("num");
        Date date = jsonObj.getDate("date");

        List<String> verList =
                Arrays.asList(
                        "3.1.0", "3.2.2", "3.2.6", "3.2.9", "3.3.0", "3.4.5", "3.4.7", "3.5.0");

        List<Integer> netList = Arrays.asList(0, 1, 2, 3, 4, 5);
        List<String> brandList =
                Arrays.asList("iphone", "xiaomi", "huawei", "sanxing", "vivo", "oppo");
        List<String> displayList =
                Arrays.asList("1366*768", "375*667", "414*896", "390*844", "360*740", "912*1368");
        List<String> osverList =
                Arrays.asList("ios10", "ios12", "miui8", "android12", "mui12", "harmonyOS2");

        List<D1wh1Response.UserAction> userActionList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            D1wh1Response.UserAction ua = new D1wh1Response.UserAction();
            ua.setUid(RandomUtils.nextInt(10000, 20000));
            ua.setXaid("ab25617-c38910-m" + RandomUtils.nextInt(0, 10000));
            ua.setPlatform(RandomUtils.nextInt(1, 4));
            ua.setVer(verList.get(RandomUtils.nextInt(0, verList.size())));
            ua.setVercode(ua.getVer().replace(".", "") + RandomUtils.nextInt(1, 10000));
            ua.setNet(netList.get(RandomUtils.nextInt(0, netList.size())));
            ua.setBrand(brandList.get(RandomUtils.nextInt(0, brandList.size())));
            ua.setModel(ua.getBrand() + RandomUtils.nextInt(1, 10));
            ua.setDisplay(displayList.get(RandomUtils.nextInt(0, displayList.size())));
            ua.setOsver(osverList.get(RandomUtils.nextInt(0, osverList.size())));

            List<BaseActVO> actList = new ArrayList<>();
            int subNum = RandomUtils.nextInt(1, 10);
            for (int j = 0; j < subNum; j++) {
                int k = RandomUtils.nextInt(1, 6);
                switch (k) {
                    case 1:
                        actList.add(Act1VO.getInstance());
                        break;
                    case 2:
                        actList.add(Act2VO.getInstance());
                        break;
                    case 3:
                        actList.add(Act3VO.getInstance());
                        break;
                    case 4:
                        actList.add(Act4VO.getInstance());
                        break;
                    case 5:
                        actList.add(Act5VO.getInstance());
                        break;
                }
            }
            ua.setData(actList);
            userActionList.add(ua);
        }

        response.setData(userActionList);
        return response;
    }
}
