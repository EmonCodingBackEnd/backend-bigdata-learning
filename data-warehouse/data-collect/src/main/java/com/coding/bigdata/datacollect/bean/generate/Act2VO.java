package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomUtils;

import java.util.Date;

@Getter
@Setter
public class Act2VO extends BaseActVO {

    private static final long serialVersionUID = 3407261119748250830L;
    /** 商品ID. */
    private Long goods_id;

    /** 商品展示顺序：在列表页中排第几位，从0开始. */
    private Integer location;

    public static Act2VO getInstance() {
        Act2VO act = new Act2VO();
        act.setAct(2);
        act.setActtime(new Date().getTime());
        act.setGoods_id(RandomUtils.nextLong(28612038928L, 30000000000L));
        act.setLocation(RandomUtils.nextInt(0, 20));
        return act;}
}
