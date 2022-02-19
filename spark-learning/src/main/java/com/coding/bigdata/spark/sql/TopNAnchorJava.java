package com.coding.bigdata.spark.sql;

import com.coding.bigdata.common.EnvUtils;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*
 * 需求：计算TopN主播
 * 1、直接使用SparkSession中的load方式加载json数据
 * 2、对这两份数据注册临时表
 * 3、执行sql计算TopN主播
 * 4、使用foreach将结果打印到控制台
 */
public class TopNAnchorJava {

    @SuppressWarnings("all")
    public static void main(String[] args) {
        // 创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession =
                EnvUtils.buildSparkSessionByEnv(SqlDemoJava.class.getSimpleName());

        String videoInfoPath = "custom/data/spark/topN/input/video_info.log";
        String giftRecordPath = "custom/data/spark/topN/input/gift_record.log";
        String top3Path = "custom/data/spark/topN/output";
        if (!EnvUtils.isWin) {
            videoInfoPath = EnvUtils.toHDFSPath(videoInfoPath);
            giftRecordPath = EnvUtils.toHDFSPath(giftRecordPath);
            top3Path = EnvUtils.toHDFSPath(top3Path);
        }

        // 1、直接使用SparkSession中的load方式加载json数据
        Dataset<Row> videoInfoDF = sparkSession.read().json(videoInfoPath);
        Dataset<Row> giftRecordDF = sparkSession.read().json(giftRecordPath);

        // 2、对这两份数据注册临时表
        videoInfoDF.createOrReplaceTempView("video_info");
        giftRecordDF.createOrReplaceTempView("gift_record");

        // 3、执行sql计算TopN主播
        StringBuilder sb = new StringBuilder();
        sb.append(" select t4.area, concat_ws(',' ,collect_list(topn)) as topn_list");
        sb.append(" from");
        sb.append(" (");
        sb.append(" select");
        sb.append(" t3.area, concat(t3.uid, ':', cast(t3.gold_sum_all as int)) as topn");
        sb.append(" from");
        sb.append(" (");
        sb.append(
                "  select t2.uid, t2.area, t2.gold_sum_all, row_number() over (partition by area order by gold_sum_all desc) as num");
        sb.append("  from");
        sb.append("  (");
        sb.append("    select t1.uid, max(t1.area) as area, sum(t1.gold_sum) as gold_sum_all");
        sb.append("    from");
        sb.append("    (");
        sb.append("      select vi.uid,vi.vid,vi.area,gr.gold_sum from video_info as vi");
        sb.append("      join");
        sb.append("      (select vid, sum(gold) as gold_sum from gift_record group by vid) as gr");
        sb.append("      on vi.vid = gr.vid");
        sb.append("    ) as t1");
        sb.append("    group by t1.uid");
        sb.append("  ) as t2");
        sb.append(" ) as t3");
        sb.append(" where t3.num <= 3");
        sb.append(" ) as t4");
        sb.append(" group by t4.area");
        String sql = sb.toString();
        Dataset<Row> resDF = sparkSession.sql(sql);

        // 4、使用foreach将结果打印到控制台
        if (EnvUtils.isWin) {
            resDF.foreach(
                    (ForeachFunction<Row>)
                            row -> System.out.println(row.getString(0) + "\t" + row.getString(1)));
        } else {
            // 指定HDFS的路径信息即可，需要指定一个不存在的目录
            EnvUtils.checkOutputPath(top3Path, false);
            resDF.javaRDD()
                    .map(
                            row ->
                                    row.getAs("area").toString()
                                            + "\t"
                                            + row.getAs("topn_list").toString())
                    .saveAsTextFile(top3Path);
        }

        sparkSession.stop();
    }
}
