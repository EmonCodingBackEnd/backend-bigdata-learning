package com.coding.bigdata.hadoop.mr;

import lombok.Getter;
import lombok.Setter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** Java中的序列化 */
public class JavaSerialize {
    public static void main(String[] args) throws IOException {
        // 创建Student对象，并设置id和name属性
        StudentJava studentJava = new StudentJava();
        studentJava.setId(1L);
        studentJava.setName("Hadoop");

        // 将Student对象的当前状态写入本地文件中
        FileOutputStream fos = new FileOutputStream("custom/data/mr/serial/output/student_java.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(studentJava);
        oos.close();
        fos.close();
    }
}

@Getter
@Setter
class StudentJava implements Serializable {

    private static final long serialVersionUID = 2724721577681008351L;

    private Long id;

    private String name;
}
