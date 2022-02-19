package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomUtils;

import java.util.Date;

@Getter
@Setter
public class Act3VO extends BaseActVO {

    private static final long serialVersionUID = -4811333494640543601L;
    /** 商品ID. */
    private Long goods_id;

    /** 页面停留时长（单位毫秒）. */
    private Integer stay_time;

    /** 页面加载耗时（单位毫秒）. */
    private Integer loading_time;

    public static Act3VO getInstance() {
        Act3VO act = new Act3VO();
        act.setAct(3);
        act.setActtime(new Date().getTime());
        act.setGoods_id(RandomUtils.nextLong(28612038928L, 30000000000L));
        act.setLoading_time(RandomUtils.nextInt(20, 3500));
        return act;
    }
}
