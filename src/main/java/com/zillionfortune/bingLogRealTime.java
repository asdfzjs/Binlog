package com.zillionfortune;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;

/**
 * Created by daniel on 17-1-12.
 */
public class bingLogRealTime {
    public static void main(String args[]) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.10.61",
                11111), "example", "", "");
        int batchSize = 1000;

        Connection mysqlCon = null;  //创建用于连接数据库的Connection对象
        try {

            Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动
            mysqlCon = DriverManager.getConnection(
                    "jdbc:mysql://192.168.10.61:3306/cmcs_test?useUnicode=true&characterEncoding=utf8", "test", "test");// 创建数据连接

        } catch (Exception e) {
            System.out.println("数据库连接失败" + e.getMessage());
        }

        try {
            mysqlCon.setAutoCommit(false);
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    messageCount(message.getEntries(),mysqlCon);
                }

                connector.ack(batchId); // 提交确认
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("============================================================connect crash");
        }  finally {
            connector.disconnect();
            if(mysqlCon != null){
                try {
                    mysqlCon.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private static void messageCount(List<CanalEntry.Entry> entrys,Connection mysqlCon) throws SQLException {
        try {


            MessageCount mc = new MessageCount();

            for (CanalEntry.Entry entry : entrys) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    continue;
                }

                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                            e);
                }

                EventType eventType = rowChage.getEventType();

                System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                        entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                        entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                        eventType));



                for (RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == EventType.INSERT) {

                        if("CyOrders".equals(entry.getHeader().getTableName())){
                            //如果是CyOrders
                            mc.businessAdd2FutureAssetSituation(rowData.getAfterColumnsList(),mysqlCon,1);
                            mc.businessAdd2FutureAssetSituationForecast(rowData.getAfterColumnsList(),mysqlCon,1);
                        }else if ("ziminDetail".equals(entry.getHeader().getTableName())){
                            //如果是zimiDetail
                            mc.assetAdd2FutureAssetSituation(rowData.getAfterColumnsList(),mysqlCon,4,1);
                            mc.assetAdd2FutureAssetSituationForecast(rowData.getAfterColumnsList(),mysqlCon,4,1);
                        }else if ("msdDetail".equals(entry.getHeader().getTableName())){
                            //如果是msDetail
                            mc.assetAdd2FutureAssetSituation(rowData.getAfterColumnsList(),mysqlCon,3,1);
                            mc.assetAdd2FutureAssetSituationForecast(rowData.getAfterColumnsList(),mysqlCon,3,1);
                        }

                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        if("CyOrders".equals(entry.getHeader().getTableName())){
                            //如果是CyOrders
                            mc.businessUpdate2FutureAssetSituation(rowData.getAfterColumnsList(),rowData.getBeforeColumnsList(),mysqlCon);
                            mc.businessUpdate2FutureAssetSituationForecast(rowData.getAfterColumnsList(),rowData.getBeforeColumnsList(),mysqlCon);
                        }else if ("ziminDetail".equals(entry.getHeader().getTableName())){
                            //如果是zimiDetail
                            mc.assetUpdate2FutureAssetSituation(rowData.getAfterColumnsList(),rowData.getBeforeColumnsList(),mysqlCon,4);
                            mc.assetUpdate2FutureAssetSituationForecast(rowData.getAfterColumnsList(),rowData.getBeforeColumnsList(),mysqlCon,4);
                        }else if ("msdDetail".equals(entry.getHeader().getTableName())){
                            //如果是msDetail
                            mc.assetUpdate2FutureAssetSituation(rowData.getAfterColumnsList(),rowData.getBeforeColumnsList(),mysqlCon,3);
                            mc.assetUpdate2FutureAssetSituationForecast(rowData.getAfterColumnsList(),rowData.getBeforeColumnsList(),mysqlCon,3);
                        }
                    }
                }
                mysqlCon.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                mysqlCon.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
