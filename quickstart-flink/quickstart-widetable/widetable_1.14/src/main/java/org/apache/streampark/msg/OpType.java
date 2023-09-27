package org.apache.streampark.msg;

import lombok.Getter;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public enum OpType {

    INSERT("I", "INSERT"), UPDATE("U", "UPDATE"), DELETE("D", "DELETE");

    private String[] code;

    private final static Map<String, OpType> opTypes = Stream.of(OpType.values())
            .flatMap(item -> Stream.of(item.getCode()).map(code -> Entry.<String, OpType>builder().key(code).value(item).build()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    OpType(String... code) {
        this.code = code;
    }

    public static OpType getOpType(String str) {
        return opTypes.get(str);
    }

    public static void main(String[] args) {
        System.out.println(opTypes);
    }
}
