package com.coding.bigdata.hadoop.mr;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.*;

/** Hadoop序列化机制 */
public class HadoopSerialize {
    public static void main(String[] args) throws IOException {
        // 创建Student对象，并设置id和name属性
        StudentWritable studentWritable = new StudentWritable();
        studentWritable.setId(1L);
        studentWritable.setName("Hadoop");

        // 将Student对象的当前状态写入本地文件中
        FileOutputStream fos = new FileOutputStream("custom/data/mr/serial/output/student_writable.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        studentWritable.write(oos);
        oos.close();
        fos.close();
    }
}

@Getter
@Setter
class StudentWritable implements Writable {

    private Long id;

    private String name;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();
    }
}
