package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Date;

@Getter
@Setter
public class Act5VO extends BaseActVO {

    private static final long serialVersionUID = -9117562541135296885L;
    /** 商品ID. */
    private String err_code;

    /** 页面加载耗时（单位毫秒）. */
    private Integer loading_time;

    /** 加载类型： 1-读缓存 2-请求接口 */
    private Integer loading_type;

    /** 列表页加载商品数量. */
    private Integer goods_num;

    public static Act5VO getInstance() {
        Act5VO act = new Act5VO();
        act.setAct(4);
        act.setActtime(new Date().getTime());
        act.setErr_code(RandomStringUtils.randomAlphanumeric(10));
        return act;
    }
}
