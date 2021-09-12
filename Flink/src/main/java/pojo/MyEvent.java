package pojo;

public class MyEvent {

    public boolean hasWatermarkMarker() {
        return true;
    }

    public long getWatermarkTimestamp() {
        return 1L;
    }
}
