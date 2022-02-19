package com.coding.bigdata.useraction.utils;

/*
 * 生成日期和act数组
 */
public class GenerateDateAndActArrUtils {

    public static String[] getDateArr() {
        String[] dateArr = new String[28];
        for (int i = 0; i < 28; i++) {
            String d_tmp = "2026-02-";
            if (i < 9) {
                d_tmp += "0" + (i + 1);
            } else {
                d_tmp += (i + 1);
            }
            dateArr[i] = d_tmp;
        }
        return dateArr;
    }

    public static int[] getActArr() {
        return new int[] {1, 2, 3, 4, 5};
    }

    public static String[] getTableArr() {
        return new String[] {
            "user",
            "user_extend",
            "user_addr",
            "goods_info",
            "category_code",
            "user_order",
            "order_item",
            "order_delivery",
            "payment_flow"
        };
    }

    public static String[] getZipDateArr() {
        return new String[] {"2026-03-01", "2026-03-02", "2026-03-03"};
    }
}
