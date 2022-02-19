package com.coding.bigdata.datacollect.bean.generate;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomUtils;

import java.util.Date;

@Getter
@Setter
public class Act1VO extends BaseActVO {

    private static final long serialVersionUID = 4740381609366991251L;
    /** 开屏广告展示状态：1-成功 2-失败 */
    private Integer ad_status;

    /** 开屏广告加载耗时（单位毫秒）. */
    private Integer loading_time;

    public static Act1VO getInstance() {
        Act1VO act = new Act1VO();
        act.setAct(1);
        act.setAd_status(RandomUtils.nextInt(0, 2));
        act.setActtime(new Date().getTime());
        act.setLoading_time(RandomUtils.nextInt(100, 3000));
        return act;
    }
}
