package utils;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * @author yang 2021/10/25
 */
public class WearStatePredictSink extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;
    private Booster xgb;
    private MlflowRestApi mlflowRestApi;
    @Override
    public void open(Configuration parameters) throws Exception {
        mlflowRestApi = new MlflowRestApi();
        xgb = XGBoost.loadModel("modelTraining/xgb.model");
        super.open(parameters);
    }
    @Override
    public void invoke(Row r, Context context) {
        try {
            String id = r.getFieldAs(0);
            Timestamp ts = Timestamp.valueOf(r.getField(1).toString().replace("T"," "));
            //'x_avg','y_avg','z_avg','vb_avg','x_stddev','y_stddev','z_stddev','vb_stddev','x_var','y_var','z_var','vb_var','x_rms','y_rms','z_rms','vb_rms'
            float[] df =  new float[16];
            df[0] = r.getFieldAs("x_avg");
            df[1] = r.getFieldAs("y_avg");
            df[2] = r.getFieldAs("z_avg");
            df[3] = r.getFieldAs("vb_avg");
            df[4] = r.getFieldAs("x_stddev");
            df[5] = r.getFieldAs("y_stddev");
            df[6] = r.getFieldAs("z_stddev");
            df[7] = r.getFieldAs("vb_stddev");
            df[8] = r.getFieldAs("x_var");
            df[9] = r.getFieldAs("y_var");
            df[10] = r.getFieldAs("z_var");
            df[11] = r.getFieldAs("vb_var");
            df[12] = r.getFieldAs("x_rms");
            df[13] = r.getFieldAs("y_rms");
            df[14] = r.getFieldAs("z_rms");
            df[15] = r.getFieldAs("vb_rms");
            DMatrix dt = new DMatrix(df,1,df.length,0);
            //invoke local predication
            float[][] pt = xgb.predict(dt);
            String predictState = this.predictState((int) pt[0][0]);
            String predictStateRemotely = "";
            try{
                //invoke remote predication by http rest api
                int rpt = mlflowRestApi.mlPredict(df);
                predictStateRemotely = this.predictState(rpt);
            }catch(Exception e){
                e.printStackTrace();
            }
            System.out.println("**************--"+ts+"; Machine Id:"+id+"; Original State:"+ r.getFieldAs("state")+"; Local prediction:" + predictState+"; Remote prediction:" + predictStateRemotely+" --*************");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String predictState(int state){
        String predictState = "predict failed";
        switch (state) {
            case 0:
                predictState = "new";
                break;
            case 1:
                predictState = "initial";
                break;
            case 2:
                predictState = "normal";
                break;
            case 3:
                predictState = "accelerated";
                break;
        }
        return predictState;
    }

    @Override
    public void close() throws Exception {

    }
}
