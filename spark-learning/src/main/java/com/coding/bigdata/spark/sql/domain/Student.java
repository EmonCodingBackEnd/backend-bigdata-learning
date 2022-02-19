package com.coding.bigdata.spark.sql.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
public class Student implements Serializable {
    private static final long serialVersionUID = -454375298179178362L;

    private String name;
    private Integer age;

    @Override
    public String toString() {
        return "Student{" + "name='" + name + '\'' + ", age=" + age + '}';
    }
}
