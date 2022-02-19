package com.coding.bigdata.hadoop.hdfs;

import org.junit.jupiter.api.*;

@DisplayName("Junit5演示")
public class Junit5Tests {

    @BeforeAll
    static void beforeAll() {
        System.out.println("初始化数据");
    }

    @AfterAll
    static void afterAll() {
        System.out.println("清理数据");
    }

    @BeforeEach
    void setUp() {
        System.out.println("当前测试方法开始");
    }

    @AfterEach
    void tearDown() {
        System.out.println("当前测试方法结束");
    }

    @Test
    void testDemo1() {
        System.out.println("测试方法1执行");
    }

    @Test
    void testDemo2() {
        System.out.println("测试方法2执行");
    }
}
