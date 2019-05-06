package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 类的注释
 *
 * @Package hbase
 * @ClassName HbaseClient
 * @Description 连接Hbase
 * @Author liyuzhi
 * @Date 2018-05-12 21:54
 */

public class HbaseClient {
    public static void main(String[] args) throws IOException {
        Connection conn = getConnection();
        Table user = getTableByTableName(conn);
        put(user);
        conn.close();
    }


    //Hbase里的scan api 条件扫描
    private static void scanOther(Table user) throws IOException {
        Scan scan1 = new Scan();
        ResultScanner scanner1 = user.getScanner(scan1); // co ScanExample-2-GetScanner Get a scanner to iterate over the rows.
        for (Result res : scanner1) {
            System.out.println(res);
        }
        scanner1.close();

        System.out.println("Scanning table #2...");
        Scan scan2 = new Scan();
        scan2.addFamily(Bytes.toBytes("colfam1"));
        ResultScanner scanner2 = user.getScanner(scan2);
        for (Result res : scanner2) {
            System.out.println(res);
        }
        scanner2.close();

        System.out.println("Scanning table #3...");
        Scan scan3 = new Scan();
        scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
                addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-33")).
                setStartRow(Bytes.toBytes("row-10")).
                setStopRow(Bytes.toBytes("row-20"));
        ResultScanner scanner3 = user.getScanner(scan3);
        for (Result res : scanner3) {
            System.out.println(res);
        }
        scanner3.close();


        System.out.println("Scanning table #4...");

        Scan scan4 = new Scan();
        scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
                setStartRow(Bytes.toBytes("row-10")).
                setStopRow(Bytes.toBytes("row-20"));
        ResultScanner scanner4 = user.getScanner(scan4);
        for (Result res : scanner4) {
            System.out.println(res);
        }
        scanner4.close();


        System.out.println("Scanning table #5...");

        Scan scan5 = new Scan();

        scan5.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
                setStartRow(Bytes.toBytes("row-20")).
                setStopRow(Bytes.toBytes("row-10")).
                setReversed(true);
        ResultScanner scanner5 = user.getScanner(scan5);
        for (Result res : scanner5) {
            System.out.println(res);
        }
        scanner5.close();

        user.close();
    }

    //Hbase里的scan api 全表扫描
    private static void scanAll(Table user) throws IOException {
        Scan scan = new Scan();

        ResultScanner scanner = user.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result.listCells());
        }
        user.close();
    }

    //Hbase里的delete api
    private static void delete(Table user) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("10001"));
        delete.addFamily(Bytes.toBytes("info"));
        user.delete(delete);
        user.close();
    }

    //Hbase里的put api
    private static void put(Table user) throws IOException {
        try {
            Put put = new Put(Bytes.toBytes("10000000000"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("liyuzhi"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(20));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addrees"), Bytes.toBytes("dashiqiao"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("phone"), Bytes.toBytes(12345));
            user.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(user);
        }

        user.close();
    }

    //Hbase里的get api
    private static void get(Table user) throws IOException {

        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        Result result = user.get(get);
        byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        System.out.println("Value: " + Bytes.toString(val));

    }

    //Hbase里的get api
    private static void getList(Table user) throws IOException {

        byte[] cf1 = Bytes.toBytes("colfam1");
        byte[] qf1 = Bytes.toBytes("qual1");
        byte[] qf2 = Bytes.toBytes("qual2");
        byte[] row1 = Bytes.toBytes("row1");
        byte[] row2 = Bytes.toBytes("row2");

        List<Get> gets = new ArrayList<Get>();

        Get get1 = new Get(row1);
        get1.addColumn(cf1, qf1);
        gets.add(get1);

        Get get2 = new Get(row2);
        get2.addColumn(cf1, qf1);
        gets.add(get2);

        Get get3 = new Get(row2);
        get3.addColumn(cf1, qf2);
        gets.add(get3);

        Result[] results = user.get(gets);

        System.out.println("First iteration...");
        for (Result result : results) {
            String row = Bytes.toString(result.getRow());
            System.out.print("Row: " + row + " ");
            byte[] val = null;
            if (result.containsColumn(cf1, qf1)) {
                val = result.getValue(cf1, qf1);
                System.out.println("Value: " + Bytes.toString(val));
            }
            if (result.containsColumn(cf1, qf2)) {
                val = result.getValue(cf1, qf2);
                System.out.println("Value: " + Bytes.toString(val));
            }
        }

        System.out.println("Second iteration...");
        for (Result result : results) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        "Row: " + Bytes.toString(
                                cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + // co GetListExample-7-GetValue2 Two different ways to access the cell data.
                                " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        System.out.println("Third iteration...");
        for (Result result : results) {
            System.out.println(result);
        }
        user.close();


    }

    //初始化连接
    private static Connection getConnection() throws IOException {
        Configuration configuraton = HBaseConfiguration.create();
        return ConnectionFactory.createConnection(configuraton);
    }

    //设置要操作的表
    private static Table getTableByTableName(Connection conn) throws IOException {
        return conn.getTable(TableName.valueOf("user"));
    }

}
