package xy;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import utils.*;

/**
 * @author yang 2021-10-18
 */
public class MachineToolWearDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //1.get sensor data
        //Axis-X, frequency 100hz, four group:id=1,2,3,4
        DataStream<MachineToolWearData> wearDataStreamX = env
                .addSource(new MachineToolWearDataSource("MachineToolWearDataX.csv"))
                .name("wearDataX");
        //Axis-Y, frequency 100hz, four group:id=1,2,3,4
        DataStream<MachineToolWearData> wearDataStreamY = env
                .addSource(new MachineToolWearDataSource("MachineToolWearDataY.csv"))
                .name("wearDataY");
        //Axis-Z, frequency 100hz, four group:id=1,2,3,4
        DataStream<MachineToolWearData> wearDataStreamZ = env
                .addSource(new MachineToolWearDataSource("MachineToolWearDataZ.csv"))
                .name("wearDataZ");
        //Vibrant, frequency 100hz, four group:id=1,2,3,4
        DataStream<MachineToolWearData> wearDataStreamVB = env
                .addSource(new MachineToolWearDataSource("MachineToolWearDataVB.csv"))
                .name("wearDataVB");

        //2. union four different datasource
        //flink handles 100hz*4groups*4sensors=1600 data points every second.
        DataStream<MachineToolWearData> unionWearDataStream = wearDataStreamX.union(wearDataStreamY, wearDataStreamZ,wearDataStreamVB);

        //3. create a table
        //Table wearDataTable = tEnv.fromDataStream(unionWearDataStream).as("id","axis", "data","event_time","state");

        Table wearDataTable =
                tEnv.fromDataStream(
                        unionWearDataStream,
                        Schema.newBuilder()
                                .column("id","STRING")
                                .column("axis","STRING")
                                .column("data","FLOAT")
                                .column("event_time", "TIMESTAMP_LTZ(3)")
                                .column("state", "STRING")
                                .watermark("event_time", "event_time - INTERVAL '2' SECOND")
                                .build());
        wearDataTable.printSchema();

        //4. create a view
        tEnv.createTemporaryView("wear_data",wearDataTable);
        tEnv.from("wear_data").printSchema();

        //5. features extraction
        //5.1calculate avg every 2 seconds
        String sqlQ1 = "SELECT id, axis, event_time,data,AVG(data) OVER w avg_data " +
                "FROM wear_data " +
                "WINDOW w AS(PARTITION BY id,axis ORDER BY event_time " +
                "RANGE BETWEEN INTERVAL '2' SECOND PRECEDING AND CURRENT ROW)";

        /*5.2
         * features extraction every 2 seconds using --tumbling windows--
         * feature1: Average
         * feature2: Population standard deviation
         * feature3: Cumulative standard deviation
         * feature4: Population variance
         * feature5: Sample variance
         * */
        String sqlQ2 =
                "SELECT id, axis, " +
                        "  TUMBLE_START(event_time, INTERVAL '2' SECOND) as wStart,  " +
                        "  AVG(data) as avg_data," +
                        "  STDDEV_POP(data) as stddev_pop_data," +
                        "  STDDEV_SAMP(data) as stddev_samp_data," +
                        "  VAR_POP (data) as var_pop_data," +
                        "  VAR_SAMP (data) as var_samp_data" +
                        "FROM wear_data " +
                        "GROUP BY TUMBLE(event_time, INTERVAL '2' SECOND), id, axis";

        /*5.3
        * features extraction every 2 seconds using 3 seconds --sliding windows--
        * feature1: Mean
        * feature2: Population standard deviation
        * feature3: Cumulative standard deviation
        * feature4: Population variance
        * feature5: Sample variance
        * */
        String sqlQ3 =
                "SELECT id, axis, " +
                        "  HOP_START(event_time, INTERVAL '2' SECOND, INTERVAL '3' SECOND) as wStart,  " +
                        "  AVG(data) as avg_data," +
                        "  STDDEV_POP(data) as stddev_pop_data," +
                        "  STDDEV_SAMP(data) as stddev_samp_data," +
                        "  VAR_POP (data) as var_pop_data," +
                        "  VAR_SAMP (data) as var_samp_data " +
                        "FROM wear_data " +
                        "GROUP BY HOP(event_time, INTERVAL '2' SECOND ,INTERVAL '3' SECOND), id, axis";


        /*5.4
         * features extraction, calculate using --complex sql query--, within every 2 seconds using 3 seconds sliding windows
         * feature1: Mean
         * feature2: Population standard deviation
         * feature3: Cumulative standard deviation
         * feature4: Population variance
         * feature5: Sample variance
         * feature6: Root mean square
         * feature7: Peak
         * feature8: Crest factor
         * feature9: Kurtosis
         * feature10: Form factor
         * feature11: Pulse factor
         * feature12: Margin factor
         * */
        String sqlQ4 =
                "SELECT id,axis, "+
                    " HOP_START(event_time, INTERVAL '2' SECOND, INTERVAL '4' SECOND) as wStart, "+
                    " AVG(data) as avg_data, "+ //feature1
                    " STDDEV_POP(data) as stddev_pop_data, "+//feature2
                    " STDDEV_SAMP(data) as stddev_samp_data, "+//feature3
                    " VAR_POP(data) as var_pop_data, "+//feature4
                    " VAR_SAMP(data) as var_samp_data, "+//feature5
                    " (max(data)-min(data))/2 RMS, "+ //feature6
                    "  sqrt(sum(POWER(data,2)))/count(data) PEAK, "+ //feature7
                    "  ((max(data)-min(data))/2)/(sqrt(sum(POWER(data,2)))/count(data)) as Cf, "+ //feature8
                    " sum(POWER(data,4))/(POWER(sqrt(sum(POWER(data,2)))/count(data),4)*count(data)) as Kv, "+ //feature9
                    " ((sqrt(sum(POWER(data,2)))/count(data))*count(data))/sum(abs(data)) as S, "+ //feature10
                    " (((max(data)-min(data))/2)*count(data))/sum(abs(data)) as If_, "+ //feature11
                    " ((max(data)-min(data))/2)/POWER(sum(sqrt(abs(data)))/count(data),2) as CLf, "+ //feature12
                    " max(state) state"+
                " FROM wear_data "+
                " GROUP BY HOP(event_time, INTERVAL '2' SECOND ,INTERVAL '4' SECOND), id, axis";

        /*5.5
         * features extraction using user defined function
         */
        tEnv.createTemporarySystemFunction("countUdaf",new CountUDAF());

        String sqlQ5 =  "SELECT id, axis, event_time,data," +
                "AVG(data) OVER w avg_data," +
                "countUdaf(data) OVER w count_ " +
                "FROM wear_data " +
                "WINDOW w AS(PARTITION BY id,axis ORDER BY event_time " +
                "RANGE BETWEEN INTERVAL '2' SECOND PRECEDING AND CURRENT ROW)";

        Table resultTable = tEnv.sqlQuery(sqlQ4);
        // interpret the insert-only Table as a DataStream again
        DataStream<Row> resultStream = tEnv.toDataStream(resultTable);
        // add a printing sink and execute in DataStream API
        resultStream.print();
        //save data to DB
        // resultStream.addSink(new PostgreSQLSink());
        Table ftable =
                tEnv.fromDataStream(
                        resultStream,
                        Schema.newBuilder()
                                .columnByExpression("rowtime", "CAST(wStart AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                                .build());
        ftable.printSchema();

        // create a feature view
        tEnv.createTemporaryView("wear_data_feature",ftable);
        tEnv.from("wear_data_feature").printSchema();

        String sqlF1 = "select id, " +
                "TUMBLE_START(rowtime, INTERVAL '2' SECOND) as rstart, " +
                "sum (case when axis = 'X' then avg_data else 0 end) as x_avg, " +
                "sum (case when axis = 'Y' then avg_data else 0 end) as y_avg, " +
                "sum (case when axis = 'Z' then avg_data else 0 end) as z_avg, " +
                "sum (case when axis = 'VB' then avg_data else 0 end) as vb_avg, " +
                "sum (case when axis = 'X' then stddev_pop_data else 0 end) as x_stddev, " +
                "sum (case when axis = 'Y' then stddev_pop_data else 0 end) as y_stddev, " +
                "sum (case when axis = 'Z' then stddev_pop_data else 0 end) as z_stddev, " +
                "sum (case when axis = 'VB' then stddev_pop_data else 0 end) as vb_stddev, " +
                "sum (case when axis = 'X' then var_pop_data else 0 end) as x_var, " +
                "sum (case when axis = 'Y' then var_pop_data else 0 end) as y_var, " +
                "sum (case when axis = 'Z' then var_pop_data else 0 end) as z_var, " +
                "sum (case when axis = 'VB' then var_pop_data else 0 end) as vb_var, " +
                "sum (case when axis = 'X' then RMS else 0 end) as x_rms, " +
                "sum (case when axis = 'Y' then RMS else 0 end) as y_rms, " +
                "sum (case when axis = 'Z' then RMS else 0 end) as z_rms, " +
                "sum (case when axis = 'VB' then RMS else 0 end) as vb_rms, " +
                "max(state) as state " +
                "from wear_data_feature " +
                "group by TUMBLE(rowtime, INTERVAL '2' SECOND), id";

        Table featureTable = tEnv.sqlQuery(sqlF1);
        DataStream<Row> featureStream = tEnv.toChangelogStream(featureTable);
        featureStream.addSink(new WearStatePredictSink());
        featureStream.print();
        //try (CloseableIterator<Row> it = featureTable.execute().collect()) {
        //    while(it.hasNext()) {
        //          Row row = it.next();
        //          String s = row.getFieldAs(1).toString();
        //          System.out.println(s);
        //    }
        //}

        env.execute("MachineToolWearDetection");
    }
}
