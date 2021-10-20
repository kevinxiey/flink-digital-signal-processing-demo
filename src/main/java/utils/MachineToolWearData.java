package utils;

import java.time.Instant;
/**
 * @author 2021-10-18
 */
public class MachineToolWearData {
    public String id;
    public String axis;
    public float data;
    public Instant event_time;
    public String state;

    public MachineToolWearData() {
    }

    public MachineToolWearData(String id, String axis, float data, Instant event_time, String state) {
        this.id = id;
        this.axis = axis;
        this.data = data;
        this.event_time = event_time;
        this.state = state;
    }

    public static MachineToolWearData of(String id, String axis, float data, Instant event_time, String state) {
        return new MachineToolWearData(id, axis, data, event_time, state);
    }

    @Override
    public String toString() {
        return "(" + id + "," +axis + "," + data + "," + event_time + "," +
                state +  ")";
    }
}
