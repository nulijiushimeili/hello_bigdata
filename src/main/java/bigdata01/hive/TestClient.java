package bigdata01.hive;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * create by nulijiushimeili on 2018-07-29
 */
public class TestClient {
    public static void main(String [] args){
        try {
            TTransport transport;

            transport = new TSocket("master", 9083);
            System.out.println("12");
            transport.open();
            System.out.println("334");

            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}
