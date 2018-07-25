package bigdata01.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/*
 *  hbase的增删改查
 */
public class TestHbaseClinet {

    //获取配置信息,获取表信息
    public static HTable getTable(String name) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf,name);
        return hTable;
    }

    //获取数据
    public static void getData(HTable hTable) throws IOException {
        Get get = new Get(Bytes.toBytes("rowkey001"));
        //获取列
        //get.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("col1"));
        //获取列簇
        get.addFamily(Bytes.toBytes("f1"));
        Result res = hTable.get(get);
        //print data
        for(Cell cell : res.rawCells()){
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "=>" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "=>" +
                    Bytes.toString(CellUtil.cloneValue(cell)) + "=> timestamp:" +
                    cell.getTimestamp()
            );
        }

    }

    //添加数据
    public static void putData(HTable hTable) throws IOException {
        Put put = new Put(Bytes.toBytes("rowkey001"));
        put.add(Bytes.toBytes("f1"),Bytes.toBytes("col3"),Bytes.toBytes("value003"));
        hTable.put(put);
        getData(hTable);
    }

    //删除数据
    public static void delData(HTable hTable) throws IOException {
        Delete del = new Delete(Bytes.toBytes("rowkey001"));
        del.deleteColumn(Bytes.toBytes("f1"),Bytes.toBytes("col3"));
        hTable.delete(del);
        getData(hTable);
    }

    //全表扫描
    public static void getScan(HTable hTable) throws IOException {
        Scan scan = new Scan();
        //load scan
        ResultScanner rs = hTable.getScanner(scan);
        show(rs);
    }

    //指定范围内扫描
    public static void rangeData(HTable hTable) throws IOException {
        Scan scan = new Scan();
        //只扫描列名是col1这个列的值
        //scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("col1"));
        //查询指定的行
        scan.setStartRow(Bytes.toBytes("rowkey02"));
        scan.setStopRow(Bytes.toBytes("rowkey03"));
        //load scan
        ResultScanner rs = hTable.getScanner(scan);
        show(rs);
    }


    // 展示数据
    private static void show(ResultScanner rs){
        for(Result res : rs){
            System.out.println(Bytes.toString(res.getRow()));
            for(Cell cell : res.rawCells()){
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "=>" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "=>" +
                        Bytes.toString(CellUtil.cloneValue(cell)) + "=> timestamp:" +
                        cell.getTimestamp()
                );
            }
            System.out.println("--------------------------------------------");
        }
    }


    public static void main(String[] args) throws Exception {
        //delData(getTable("t1"));
//        getScan(getTable("t1"));
//        rangeData(getTable("t1"));
//        putData(getTable("t1"));
    }

}
