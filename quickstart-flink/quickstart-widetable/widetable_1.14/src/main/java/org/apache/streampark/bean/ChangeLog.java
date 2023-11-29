package org.apache.streampark.bean;

import lombok.*;

/**
 * @author lysgithub0302
 * @note
 * @date 2023/9/22
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ChangeLog {

    private String main_key;
    private Long es;
    private Long ts;
    private Long ps;
    private String tableName;

}
