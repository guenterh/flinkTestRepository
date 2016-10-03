package gh;



    import org.apache.flink.addons.hbase.TableInputFormat;
    import org.apache.flink.api.common.functions.FilterFunction;
    import org.apache.flink.api.java.DataSet;
    import org.apache.flink.api.java.ExecutionEnvironment;
    import org.apache.flink.api.java.tuple.Tuple2;
    import org.apache.hadoop.hbase.client.Result;
    import org.apache.hadoop.hbase.client.Scan;
    import org.apache.hadoop.hbase.util.Bytes;

    import java.util.List;


/**
 * Created by swissbib on 28.09.16.
 */
public class FlinkHBaseTest {

    static long counter = 0;

    private static void test() {
        counter++;
    }


    public static void main(String[] args) throws Exception {

        long counter = 0;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        @SuppressWarnings("serial")
        DataSet<Tuple2<String, String>> hbaseDs = env.createInput(new TableInputFormat<Tuple2<String, String>>() {

            @Override
            public String getTableName() {
                return HBaseFlinkTestConstants.TEST_TABLE_NAME;
            }

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(HBaseFlinkTestConstants.CF_SOME, HBaseFlinkTestConstants.Q_SOME);
                return scan;
            }

            private Tuple2<String, String> reuse = new Tuple2<String, String>();

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result r) {
                String key = Bytes.toString(r.getRow());
                String val = Bytes.toString(r.getValue(HBaseFlinkTestConstants.CF_SOME, HBaseFlinkTestConstants.Q_SOME));
                reuse.setField(key, 0);
                reuse.setField(val, 1);
                FlinkHBaseTest.test();
                return reuse;
            }
        });
        /*
                .filter(new FilterFunction<Tuple2<String,String>>() {

                    @Override
                    public boolean filter(Tuple2<String, String> t) throws Exception {
                        String val = t.getField(1);
                        if(val.startsWith("someStr"))
                            return true;
                        return false;
                    }
                });
        */
        //List<Tuple2<String, String>> list =  hbaseDs.collect();
        //hbaseDs.writeAsCsv("/home/swissbib/temp/flink.out.txt", "\n", " ");
        hbaseDs.writeAsText("/home/swissbib/temp/flink.out");


        // kick off execution.
        env.execute ();
        System.out.println("counter: " + FlinkHBaseTest.counter);

    }

}
