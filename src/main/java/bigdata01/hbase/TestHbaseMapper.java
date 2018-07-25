package bigdata01.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHbaseMapper extends TableMapper<ImmutableBytesWritable,Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 封装put
        Put put = new Put(key.get());
        for(Cell cell : value.rawCells()){
            // 判断当前cell的列簇是否为info
            if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                // 判断cell的列簇为info的列是否为name
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    // 如果条件都满足,将cell写到新的表中
                    put.add(cell);
                }
            }
        }

        context.write(key,put);
    }
}
