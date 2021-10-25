package utils;

import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.types.Row;
import utils.BaseConf;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * @author yang 2021/10/19
 */
public class PostgreSQLSink extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(BaseConf.DRIVERNAME);
        connection = DriverManager.getConnection(BaseConf.URL, BaseConf.USERNAME, BaseConf.PASSWORD);
        String sql = "insert into weardata_feature(id,axis,wstart,avg_data,stddev_pop_data,stddev_samp_data,var_pop_data,var_samp_data," +
                "rms,peak,cf,kv,s,if_,clf,state) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }
    @Override
    public void invoke(Row r, Context context) {
        try {
            preparedStatement.setString(1, r.getFieldAs(0));
            preparedStatement.setString(2, r.getFieldAs(1));
            preparedStatement.setTimestamp(3, Timestamp.valueOf(r.getField(2).toString().replace("T"," ")));
            preparedStatement.setFloat(4, r.getFieldAs(3));
            preparedStatement.setFloat(5, r.getFieldAs(4));
            preparedStatement.setFloat(6, r.getFieldAs(5));
            preparedStatement.setFloat(7, r.getFieldAs(6));
            preparedStatement.setFloat(8, r.getFieldAs(7));
            preparedStatement.setFloat(9, r.getFieldAs(8));
            preparedStatement.setDouble(10, r.getFieldAs(9));
            preparedStatement.setFloat(11, r.getFieldAs(10));
            preparedStatement.setDouble(12, r.getFieldAs(11));
            preparedStatement.setDouble(13, r.getFieldAs(12));
            preparedStatement.setFloat(14, r.getFieldAs(13));
            preparedStatement.setFloat(15, r.getFieldAs(14));
            preparedStatement.setString(16, r.getFieldAs(15));
            //System.out.println("Start insert");
            preparedStatement.executeUpdate();
            //preparedStatement.addBatch();
            //preparedStatement.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {

        if (preparedStatement != null) {
            preparedStatement.close();
        }

        if (connection != null) {
            connection.close();
        }

    }
}
