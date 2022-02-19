package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class AppResponse<T> implements Serializable {

    private static final long serialVersionUID = 8691476258533170510L;

    /** 应答结果状态：true-成功；false-失败. */
    protected Boolean success = true;

    /** 应答结果数据. */
    protected T data;
}
