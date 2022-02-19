package com.coding.bigdata.hadoop.mr;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

/** 生成测试数据 */
public class GenerateDat {
    public static void main(String[] args) throws Exception {
        generate_140M();
        generate_141M();
        generate_1000W();
    }

    /**
     * 生成141M文件
     *
     * @throws IOException -
     */
    private static void generate_141M() throws IOException {
        String fileName = "./custom/data/mr/mrblock/input/s_name_141.dat";
        System.out.println("start: 开始生成141M文件->" + fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num = 0;
        while (num < 8221592) {
            bfw.write("zhangsan beijing");
            bfw.newLine();
            num++;
            if (num % 10000 == 0) {
                bfw.flush();
            }
        }
        System.out.println("end: 141M文件已生成");
    }

    /**
     * 生成140M文件
     *
     * @throws IOException -
     */
    private static void generate_140M() throws IOException {
        String fileName = "./custom/data/mr/mrblock/input/s_name_140.dat";
        System.out.println("start: 开始生成140M文件->" + fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num = 0;
        while (num < 8201592) {
            bfw.write("zhangsan beijing");
            bfw.newLine();
            num++;
            if (num % 10000 == 0) {
                bfw.flush();
            }
        }
        System.out.println("end: 140M文件已生成");
    }

    private static void generate_1000W() throws IOException {
        String[] numTpls = new String[100];
        Arrays.fill(numTpls, 5 + " ");
        for (int i = 0; i < 10; i++) {
            numTpls[RandomUtils.nextInt(0, 100)] = i + " ";
        }
        for (int i = 0; i < numTpls.length; i++) {
            int idx = RandomUtils.nextInt(0, 100);
            numTpls[idx] = numTpls[idx] + RandomStringUtils.randomAlphanumeric(50, 200);
        }

        String fileName = "./custom/data/mr/skew/input/hello_10000000.dat";
        System.out.println("start: 开始生成1000行包含数字的文件->" + fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num = 0;
        while (num < 10000000) {
            bfw.write(numTpls[num % numTpls.length]);
            bfw.newLine();
            num++;
            if (num % 10000 == 0) {
                bfw.flush();
            }
        }
        System.out.println("end: 1000行包含数字的文件已生成");
    }
}
