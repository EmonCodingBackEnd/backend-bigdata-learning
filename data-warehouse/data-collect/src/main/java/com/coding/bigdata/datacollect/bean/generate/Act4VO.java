package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomUtils;

import java.util.Date;

@Getter
@Setter
public class Act4VO extends BaseActVO {

    private static final long serialVersionUID = -6036238556132527702L;
    /** 商品ID. */
    private Long goods_id;

    /** 页面加载耗时（单位毫秒）. */
    private Integer loading_time;

    /** 加载类型： 1-读缓存 2-请求接口 */
    private Integer loading_type;

    /** 列表页加载商品数量. */
    private Integer goods_num;

    public static Act4VO getInstance() {
        Act4VO act = new Act4VO();
        act.setAct(4);
        act.setActtime(new Date().getTime());
        act.setGoods_id(RandomUtils.nextLong(28612038928L, 30000000000L));
        act.setLoading_time(RandomUtils.nextInt(20, 3500));
        act.setLoading_type(RandomUtils.nextInt(1, 3));
        act.setGoods_num(RandomUtils.nextInt(1, 21));
        return act;
    }
}
