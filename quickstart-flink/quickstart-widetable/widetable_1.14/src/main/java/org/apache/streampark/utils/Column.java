package org.apache.streampark.utils;

import lombok.*;

/**
 * @author lysgithub0302
 * @note
 * @date 2023/8/9
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class Column {
    private String col_name;
    private String col_type;
}
