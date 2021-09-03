package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        //通过死循环，在循环中一直监控数据库，否则当main方法执行完毕，连接就断掉了
        while (true){
            //2.通过连接器获取canal连接
            canalConnector.connect();

            //3.选择要订阅的数据库  必须写正则
            canalConnector.subscribe("gmall.*");

            //4.获取多个sql执行的结果
            Message message = canalConnector.get(100);

            //5.获取一个sql执行的结果
            List<CanalEntry.Entry> entries = message.getEntries();

            //6.判断是否有数据生成
            if (entries.size()>0){

                //7.获取每个entry
                for (CanalEntry.Entry entry : entries) {

                    //TODO 8.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //9.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //10.判断entry类型，如果是ROWDATA则处理（因为ROWDATA里面才是数据）
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){

                        //11.获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //12.反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 13.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 14.获取一个sql对应的数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        handle(tableName, eventType, rowDatasList);
                    }
                }

            }else {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }
    }

    /**
     * 解析具体的数据
     * @param tableName 表名
     * @param eventType 事件类型（新增、变化、删除、创建）
     * @param rowDatasList 一个sql所对应的多行数据
     */
    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            //获取每一条数据
            saveTokafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
        }else if("order_detail".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            //获取每一条数据
            saveTokafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }else if ("user_info".equals(tableName)&&(CanalEntry.EventType.INSERT.equals(eventType)||CanalEntry.EventType.UPDATE.equals(eventType))){
            //获取每一条数据
            saveTokafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }

    }

    private static void saveTokafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());

            //模拟网络震荡
            try {
                Thread.sleep(new Random().nextInt(5)*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //将数据发送到kafka中
            MyKafkaSender.send(kafkaTopicOrder, jsonObject.toString());
        }
    }
}
