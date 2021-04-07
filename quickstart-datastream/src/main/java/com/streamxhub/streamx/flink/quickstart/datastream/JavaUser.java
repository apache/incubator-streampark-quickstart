package com.streamxhub.streamx.flink.quickstart.datastream;

import java.io.Serializable;

class JavaUser implements Serializable {
    String name;
    Integer age;
    Integer gender;
    String address;

    public String toSql() {
        return String.format(
                "insert into t_user(`name`,`age`,`gender`,`address`) value('%s',%d,%d,'%s')",
                name,
                age,
                gender,
                address);
    }

}
