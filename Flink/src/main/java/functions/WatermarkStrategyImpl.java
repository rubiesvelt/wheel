package functions;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

public class WatermarkStrategyImpl implements WatermarkStrategy<Tuple3<String, Integer, Long>> {

    @Override
    public WatermarkGenerator createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }

    @Override
    public TimestampAssigner<Tuple3<String, Integer, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new RecordTimestampAssigner<>();
    }
}
