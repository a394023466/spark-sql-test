package hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 类的注释
 *
 * @Package hbase.mapreduce
 * @ClassName UserToSubuser
 * @Description
 * @Author liyuzhi
 * @Date 2018-05-13 19:15
 */

public class UserToSubuser extends Configured implements Tool {


    public static class UserInputMapper extends TableMapper<Text, Put> {
        /**
         * 方法的注释
         *
         * @return void
         * @description 从Hbase里获取数据传给Reduce，将数据插入新的表
         * @methodName map
         * @Param: key 封装了二进制的rowkey
         * @Param: value 就是rowkey下边的所有数据
         * @Param: context
         * @author liyuzhi
         * @createTime 2018-05-13 21:10
         * @version v1.0
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            //得到字符串的rowkey
            String rowkey = Bytes.toString(key.get());//封装了
            //创建一个Put对象，，传给TableReducer
            Put put = new Put(key.get());

            for (Cell cell : value.listCells()) {
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        put.add(cell);
                    }
                    if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        put.add(cell);
                    }
                }
            }

            context.write(new Text(rowkey), put);
        }
    }


    public static class UserOutputReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(null, put);
            }
        }
    }


    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());//创建Job
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        scan.setCaching(100);
        scan.setCacheBlocks(false);


        TableMapReduceUtil.initTableMapperJob("user", scan, UserInputMapper.class, Text.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("sub_user", UserOutputReducer.class, job);
        job.setNumReduceTasks(1);
        boolean states = job.waitForCompletion(true);


        return states ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();//获取配置文件
        int states = ToolRunner.run(conf, new UserToSubuser(), args);
        System.exit(states);
    }


}
