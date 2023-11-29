package org.apache.streampark.utils;


import lombok.Getter;

/**
 * @author lysgithub0302
 * @note
 * @date 2023/9/2
 */
@Getter
public enum DelType {

    DEL(1), NOTDEL( 0);

    private Integer value;
    DelType(Integer value) {
        this.value = value;
    }

}