package org.apache.streampark.reduce;

import org.apache.streampark.msg.Message;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimensionWindowReduceFunction implements ReduceFunction<Message> {

    @Override
    public Message reduce(Message value1, Message value2) throws Exception {
        Message newMessage = createMessageCopy(value2);
        updateResultListMap(newMessage, value1);
        updateResultListMap(newMessage, value2);
        return newMessage;
    }

    private Message createMessageCopy(Message originalMessage) {
        Message newMessage = Message.builder()
                .pk(originalMessage.getPk())
                .pk_col_name(originalMessage.getPk_col_name())
                .hash_pk(originalMessage.getHash_pk())
                .dbName(originalMessage.getDbName())
                .db_table(originalMessage.getDb_table())
                .tableName(originalMessage.getTableName())
                .type(originalMessage.getType())
                .data(originalMessage.getData())
                .es(originalMessage.getEs())
                .ts(originalMessage.getTs())
                .ps(originalMessage.getPs())
                .build();

        return newMessage;
    }

    private void updateResultListMap(Message message, Message value) {
        Map<String, List<Message>> resultListMap = message.getResultListMap();

        if (resultListMap == null) {
            resultListMap = new HashMap<>();
            message.setResultListMap(resultListMap);
        }

        String dbTable = value.getDb_table();

        if (!resultListMap.containsKey(dbTable)) {
            resultListMap.put(dbTable, new ArrayList<>());
        }

        resultListMap.get(dbTable).add(value);
    }
}
