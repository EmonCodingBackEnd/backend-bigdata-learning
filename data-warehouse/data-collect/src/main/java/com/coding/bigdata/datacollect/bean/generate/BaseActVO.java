package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class BaseActVO implements Serializable {

    private static final long serialVersionUID = -2680210103008890452L;
    /** 1-打开APP 2-点击商品 3-商品详情页 4-商品列表页 5-app崩溃数据 */
    private Integer act;

    /** 数据产生时间. */
    private Long acttime;
}
