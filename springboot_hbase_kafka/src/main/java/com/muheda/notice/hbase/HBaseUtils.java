package com.muheda.notice.hbase;

import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

@Slf4j
public class HBaseUtils {

    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        // 如果是集群 则主机名用逗号分隔
        configuration.set("hbase.zookeeper.quorum", "192.168.80.10");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建HBase表
     *
     * @param tableName      表名
     * @param columnFamilies 列族的数组
     */
    public static boolean createTable(String tableName, List<String> columnFamilies) {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            columnFamilies.forEach(columnFamily -> {
                ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                cfDescriptorBuilder.setMaxVersions(1);
                ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
                tableDescriptor.setColumnFamily(familyDescriptor);
            });
            admin.createTable(tableDescriptor.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 删除hBase表
     *
     * @param tableName 表名
     */
    public static boolean deleteTable(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            // 删除表前需要先禁用表
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param qualifier        列标识
     * @param value            数据
     */
    public static boolean putRow(String tableName, String rowKey, String columnFamilyName, String qualifier,
                                 String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param pairList         列标识和值的集合
     */
    public static boolean putRow(String tableName, String rowKey, String columnFamilyName, List<Pair<String, String>> pairList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            pairList.forEach(pair -> put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(pair.getKey()), Bytes.toBytes(pair.getValue())));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 根据rowKey获取指定行的数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public static Result getRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 获取指定行指定列(cell)的最新版本的数据
     *
     * @param tableName    表名
     * @param rowKey       唯一标识
     * @param columnFamily 列族
     * @param qualifier    列标识
     */
    public static String getCell(String tableName, String rowKey, String columnFamily, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                Result result = table.get(get);
                byte[] resultValue = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                return Bytes.toString(resultValue);
            } else {
                return null;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 检索全表
     *
     * @param tableName 表名
     */
    public static ResultScanner getScanner(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 检索表中指定数据
     *
     * @param tableName  表名
     * @param filterList 过滤器
     */

    public static ResultScanner getScanner(String tableName, FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 检索表中指定数据
     *
     * @param tableName   表名
     * @param startRowKey 起始RowKey
     * @param endRowKey   终止RowKey
     * @param filterList  过滤器
     */

    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey,
                                           FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除指定行记录
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 删除指定行指定列
     *
     * @param tableName  表名
     * @param rowKey     唯一标识
     * @param familyName 列族
     * @param qualifier  列标识
     */
    public static boolean deleteColumn(String tableName, String rowKey, String familyName,
                                          String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    /**
     * 根据rowkey查询记录
     *
     * @param obj
     * @param rowkey
     * @param <T>
     * @return
     */
    public <T> List<T> queryScanRowkey(T obj, String rowkey) {
        List<T> objs = new ArrayList<T>();
        String tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        ResultScanner scanner = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName)); Admin admin = connection.getAdmin()) {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(rowkey));
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T) BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("queryScanRowkey:查询失败！", e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("queryScan:关闭流异常！", e);
                }
            }
        }
        return objs;
    }

    /**
     * 获取数据（全表数据）
     *
     * @param tableName 表名
     * @return map
     */
    public static List<Map<String, String>> getData(String tableName) {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                HashMap<String, String> map = new HashMap<>();
                //rowkey
                String row = Bytes.toString(result.getRow());
                map.put("row", row);
                for (Cell cell : result.listCells()) {
                    //列族
                    String family = Bytes.toString(cell.getFamilyArray(),
                            cell.getFamilyOffset(), cell.getFamilyLength());
                    //列
                    String qualifier = Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength());
                    //值
                    String data = Bytes.toString(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    map.put(family  + qualifier, data);
                }
                list.add(map);
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 获取数据（根据rowkey）
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @return map
     */
    public static Map<String, String> getData(String tableName, String rowKey) {
        HashMap<String, String> map = new HashMap<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                for (Cell cell : result.listCells()) {
                    //列族
                    String family = Bytes.toString(cell.getFamilyArray(),
                            cell.getFamilyOffset(), cell.getFamilyLength());
                    //列
                    String qualifier = Bytes.toString(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength());
                    //值
                    String data = Bytes.toString(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    map.put(qualifier, data);
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 获取数据（根据rowkey，列族，列）
     *
     * @param tableName       表名
     * @param rowKey          rowKey
     * @param columnFamily    列族
     * @param columnQualifier 列
     * @return map
     */
    public String getData(String tableName, String rowKey, String columnFamily,
                          String columnQualifier) {
        String data = "";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
            Result result = table.get(get);
            if (result != null && !result.isEmpty()) {
                Cell cell = result.listCells().get(0);
                data = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength());
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
    /**
     * @Descripton: 创建表
     * @Author: Sorin
     * @param tableName
     * @param familyColumn
     * @Date: 2018/3/22
     */
    public void createTable(String tableName, Set<String> familyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = connection.getAdmin();) {
            HTableDescriptor htd = new HTableDescriptor(tn);
            for (String fc : familyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                htd.addFamily(hcd);
            }
            admin.createTable(htd);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("创建"+tableName+"表失败！", e);
        }
    }

    /**
     * @Descripton: 删除表
     * @Author: Sorin
     * @param tableName
     * @Date: 2018/3/22
     */
    public void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = connection.getAdmin();){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("删除"+tableName+"表失败！");
        }
    }

    /**
     * @Descripton: 根据条件过滤查询
     * @Author: Sorin
     * @param obj
     * @param param
     * @Date: 2018/3/26
     */
    public <T> List<T> queryScan(T obj, Map<String, String> param)throws Exception{
        List<T> objs = new ArrayList<T>();
        String tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName)); Admin admin = connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            Scan scan = new Scan();
            for (Map.Entry<String, String> entry : param.entrySet()){
                Class<?> clazz = obj.getClass();
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    if (!field.isAnnotationPresent(HbaseColumn.class)) {
                        continue;
                    }
                    field.setAccessible(true);
                    HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
                    String family = orm.family();
                    String qualifier = orm.qualifier();
                    if(qualifier.equals(entry.getKey())){
                        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(entry.getValue()));
                        scan.setFilter(filter);
                    }
                }
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T)BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("查询失败！");
            throw new Exception(e);
        }
        return objs;
    }

    /**
     * @Descripton: 根据rowkey查询
     * @Author: Sorin
     * @param obj
     * @param rowkeys
     * @Date: 2018/3/22
     */
    public <T> List<T> get(T obj, String ... rowkeys) {
        List<T> objs = new ArrayList<T>();
        String tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return objs;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName)); Admin admin = connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            List<Result> results = getResults(tableName, rowkeys);
            if (results.isEmpty()) {
                return objs;
            }
            for (int i = 0; i < results.size(); i++) {
                T bean = null;
                Result result = results.get(i);
                if (result == null || result.isEmpty()) {
                    continue;
                }
                try {
                    bean = HBaseBeanUtil.resultToBean(result, obj);
                    objs.add(bean);
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("查询异常！", e);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return objs;
    }


    /**
     * @Descripton: 保存实体对象
     * @Author: Sorin
     * @param objs
     * @Date: 2018/3/22
     */
    public <T> boolean save(T ... objs) {
        List<Put> puts = new ArrayList<Put>();
        String tableName = "";
        try (Admin admin = connection.getAdmin();){
            for (Object obj : objs) {
                if (obj == null) {
                    continue;
                }
                tableName = getORMTable(obj);
                // 表不存在，先获取family创建表
                if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                    // 获取family, 创建表
                    Class<?> clazz = obj.getClass();
                    Field[] fields = clazz.getDeclaredFields();
                    Set<String> set = new HashSet<>(10);
                    for(int i=0;i<fields.length;i++){
                        if (!fields[i].isAnnotationPresent(HbaseColumn.class)) {
                            continue;
                        }
                        fields[i].setAccessible(true);
                        HbaseColumn orm = fields[i].getAnnotation(HbaseColumn.class);
                        String family = orm.family();
                        if ("rowkey".equalsIgnoreCase(family)) {
                            continue;
                        }
                        set.add(family);
                    }
                    // 创建表
                    createTable(tableName, set);
                }
                Put put = HBaseBeanUtil.beanToPut(obj);
                puts.add(put);
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error("保存Hbase异常！");
        }
        return savePut(puts, tableName);
    }

    /**
     * @Descripton: 根据tableName保存
     * @Author: Sorin
     * @param tableName
     * @param objs
     * @Date: 2018/3/22
     */
    public <T> void save(String tableName, T ... objs){
        List<Put> puts = new ArrayList<Put>();
        for (Object obj : objs) {
            if (obj == null) {
                continue;
            }
            try {
                Put put = HBaseBeanUtil.beanToPut(obj);
                puts.add(put);
            } catch (Exception e) {
                log.warn("", e);
            }
        }
        savePut(puts, tableName);
    }

    /**
     * @Descripton: 删除
     * @Author: Sorin
     * @param obj
     * @param rowkeys
     * @Date: 2018/3/22
     */
    public <T> void delete(T obj, String... rowkeys) {
        String tableName = "";
        tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        List<Delete> deletes = new ArrayList<Delete>();
        for (String rowkey : rowkeys) {
            if (StringUtils.isBlank(rowkey)) {
                continue;
            }
            deletes.add(new Delete(Bytes.toBytes(rowkey)));
        }
        delete(deletes, tableName);
    }


    /**
     * @Descripton: 批量删除
     * @Author: Sorin
     * @param deletes
     * @param tableName
     * @Date: 2018/3/22
     */
    private void delete(List<Delete> deletes, String tableName) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            if (StringUtils.isBlank(tableName)) {
                log.info("tableName为空！");
                return;
            }
            table.delete(deletes);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("删除失败！",e);
        }
    }

    /**
     * @Descripton: 根据tableName获取列簇名称
     * @Author: Sorin
     * @param tableName
     * @Date: 2018/3/22
     */
    public List<String> familys(String tableName) {
        try (Table table = connection.getTable(TableName.valueOf(tableName));){
            List<String> columns = new ArrayList<String>();
            if (table==null) {
                return columns;
            }
            HTableDescriptor tableDescriptor = table.getTableDescriptor();
            HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnDescriptor :columnDescriptors) {
                String columnName = columnDescriptor.getNameAsString();
                columns.add(columnName);
            }
            return columns;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("查询列簇名称失败！" ,e);
        }
        return new ArrayList<String>();
    }

    // 保存方法
    boolean savePut(List<Put> puts, String tableName){
        if (StringUtils.isBlank(tableName)) {
            return false;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName)); Admin admin = connection.getAdmin();){
            table.put(puts);
            return true;
        }catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 获取tableName
    private String getORMTable(Object obj) {
        HbaseTable table = obj.getClass().getAnnotation(HbaseTable.class);
        return table.tableName();
    }

    // 获取查询结果
    private List<Result> getResults(String tableName, String... rowkeys) {
        List<Result> resultList = new ArrayList<Result>();
        List<Get> gets = new ArrayList<Get>();
        for (String rowkey : rowkeys) {
            if (StringUtils.isBlank(rowkey)) {
                continue;
            }
            Get get = new Get(Bytes.toBytes(rowkey));
            gets.add(get);
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName));) {
            Result[] results = table.get(gets);
            Collections.addAll(resultList, results);
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            return resultList;
        }
    }

    /**
     * @Descripton: 根据条件过滤查询（大于等于）
     * @Author: Sorin
     * @param obj
     * @param param
     * @Date: 2018/3/26
     */
    public <T> List<T> queryScanGreater(T obj, Map<String, String> param)throws Exception{
        List<T> objs = new ArrayList<T>();
        String tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName)); Admin admin = connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            Scan scan = new Scan();
            for (Map.Entry<String, String> entry : param.entrySet()){
                Class<?> clazz = obj.getClass();
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    if (!field.isAnnotationPresent(HbaseColumn.class)) {
                        continue;
                    }
                    field.setAccessible(true);
                    HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
                    String family = orm.family();
                    String qualifier = orm.qualifier();
                    if(qualifier.equals(entry.getKey())){
                        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(entry.getValue()));
                        scan.setFilter(filter);
                    }
                }
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T)BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("查询失败！");
            throw new Exception(e);
        }
        return objs;
    }
    
}
