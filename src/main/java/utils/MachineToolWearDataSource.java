package utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;


/**
 * @author yang 2021-10-18
 */
public class MachineToolWearDataSource implements SourceFunction<MachineToolWearData> {

    private boolean isRunning = true;
    private String path;
    private InputStream streamSource;

    public MachineToolWearDataSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<MachineToolWearData> sourceContext) throws Exception {
        // get raw data file from project resource folder
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String line;
        boolean isFirstLine = true;
        long timeDiff;
        Instant lastEventTs = Instant.now();
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            Instant eventTs = new Timestamp(Long.parseLong(itemStrArr[3].trim())).toInstant();
            if (isFirstLine) {
                // get timestamp from first line
                lastEventTs = eventTs;
                isFirstLine = false;
            }
            MachineToolWearData machineToolWearData = MachineToolWearData.of(itemStrArr[0],itemStrArr[1],
                    Float.parseFloat(itemStrArr[2]), eventTs,
                    itemStrArr[4]);
            //timeDiff = eventTs - lastEventTs;
            timeDiff = ChronoUnit.MILLIS.between(lastEventTs,eventTs);
           // System.out.println(timeDiff);
            if (timeDiff > 0)
                Thread.sleep(timeDiff);
            sourceContext.collect(machineToolWearData);
            lastEventTs = eventTs;
        }
    }

    // stop sending
    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        isRunning = false;
    }
}
