package org.apache.streampark.utils;

import lombok.Getter;

/**
 * @author lysgithub0302
 * @note
 * @date 2023/9/3
 */
@Getter
public enum ColType {
    INT("int"), STRING( "varchar(1000)");

    private String value;
    ColType(String value) {
        this.value = value;
    }
}
