package com.coding.bigdata.datacollect.bean;

import lombok.Getter;
import lombok.Setter;

/*
 * 接口请求返回数据
 */
@Getter
@Setter
public class Status {

    /**
     * 返回状态码.
     *
     * <p>200是成功，其他是异常
     */
    private int status = 200;

    /** 具体的错误信息 */
    private String msg = "success";
}
