package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class D1wh1Response extends AppResponse<List<D1wh1Response.UserAction>> {
    private static final long serialVersionUID = 3852139184111818378L;

    @Getter
    @Setter
    public static class UserAction implements Serializable {
        private static final long serialVersionUID = 4501688066431334614L;
        /** 用户ID. */
        private Integer uid;

        /** 手机设备ID. */
        private String xaid;

        /** 设备类型 1-Android-APP 2-IOS-APP 3-PC. */
        private int platform;

        /** 大版本号. */
        private String ver;

        /** 子版本号. */
        private String vercode;

        /** 网络类型 0-未知 1-WIFI 2-2G 3-3G 4-4G 5-5G. */
        private int net;

        /** 手机品牌. */
        private String brand;

        /** 机型. */
        private String model;

        /** 分辨率. */
        private String display;

        /** 操作系统版本号. */
        private String osver;

        /** 用户行为数据列表. */
        private List<BaseActVO> data;
    }
}
