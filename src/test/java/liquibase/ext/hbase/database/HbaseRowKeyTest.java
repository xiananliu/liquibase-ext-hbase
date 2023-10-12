package liquibase.ext.hbase.database;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseRowKeyTest {

    public static void main(String[] args) {

        byte salt = (byte) (Math.abs(Objects.hashCode(5)) % 10);
        byte[] rowKey = new byte[5];
        rowKey[0] = salt;
        System.arraycopy(Bytes.toBytes(5), 0, rowKey, 1, 4);
    }
}
