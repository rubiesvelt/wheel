import function.PseudoWindow;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.TaxiFare;

import java.util.ArrayList;
import java.util.List;

public class ProcessFunctionDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<TaxiFare> list = new ArrayList<>();

        DataStreamSource<TaxiFare> fares = env.fromCollection(list);
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .process(new PseudoWindow(Time.hours(1)));

        hourlyTips.print();
    }
}
