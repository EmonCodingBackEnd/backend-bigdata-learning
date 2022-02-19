package com.coding.bigdata.useraction.bean;

import lombok.Getter;
import lombok.Setter;

/*
 * 基础公共字段
 *
埋点上报数据基本格式：
{
"uid"：1001,  //用户ID
"xaid"："ab25617-c38910-m2991",  //手机设备ID
"platform"：2,  //设备类型, 1:Android-APP, 2:IOS-APP, 3:PC
"ver"："3.5.10",  //大版本号
"vercode"："35100083",  //子版本号
"net"：1,  //网络类型, 0:未知, 1:WIFI, 2:2G , 3:3G, 4:4G, 5:5G
"brand"："iPhone",  //手机品牌
"model"："iPhone8",  //机型
"display"："1334x750",  //分辨率
"osver"："ios13.5",  //操作系统版本号
"data"：[ //用户行为数据
	{"act"：1,"acttime"：1592486549819,"ad_status"：1,"loading_time":100},
	{"act"：2,"acttime"：1592486549819,"goods_id"："2881992"}
	]
}

act=1：打开APP
属性	含义
act	用户行为类型
acttime	数据产生时间(时间戳)
ad_status	开屏广告展示状态, 1:成功 2:失败
loading_time	开屏广告加载耗时(单位毫秒)

act=2：点击商品
属性	含义
act	用户行为类型
acttime	数据产生时间(时间戳)
goods_id	商品ID
location	商品展示顺序：在列表页中排第几位，从0开始

act=3：商品详情页
属性	含义
act	用户行为类型
acttime	数据产生时间(时间戳)
goods_id	商品ID
stay_time	页面停留时长(单位毫秒)
loading_time	页面加载耗时(单位毫秒)

act=4：商品列表页
属性	含义
act	用户行为类型
acttime	数据产生时间(时间戳)
loading_time	页面加载耗时(单位毫秒)
loading_type	加载类型：1:读缓存 2:请求接口
goods_num	列表页加载商品数量

act=5：app崩溃数据
属性	含义
act	用户行为类型
acttime	数据产生时间(时间戳)
 */
@Getter
@Setter
public class BaseField {

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
}
