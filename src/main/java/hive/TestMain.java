package hive;

import org.apache.hadoop.hive.ql.exec.spark.HiveMapFunction;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;

import java.sql.*;

/**
 * 类的注释
 *
 * @Package hive
 * @ClassName TestMain
 * @Description hive的JBDC链接
 * @Author liyuzhi
 * @Date 2018-05-21 14:58
 */

public class TestMain {
    private final static String DIRVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private final static String HIVE_PATH = "jdbc:hive2://192.168.44.132:10000/db_hive";
    private final static String USER_NAME = "root";
    private final static String PASSWORD = "cnitsec";
    public static void main(String[] args) {
        try {
            Class.forName(DIRVER_CLASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            Connection conn = DriverManager.getConnection(HIVE_PATH, USER_NAME, PASSWORD);
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from parlog");
            while (resultSet.next()) {
                String string = resultSet.getString(2);
                System.out.println(string);
            }
            statement.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
