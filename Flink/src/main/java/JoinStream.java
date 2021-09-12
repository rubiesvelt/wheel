import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;

public class JoinStream {

    public static void main(String[] args) {

        List<Tuple3<String, Integer, Long>> data = Arrays.asList(
                new Tuple3<>("ss sa", 1, 1625496302000L),
                new Tuple3<>("ss sb", 2, 1625496302000L),
                new Tuple3<>("aa qn", 4, 1625496302000L),
                new Tuple3<>("aa cui ss", 5, 1625496362000L),
                new Tuple3<>("ss", 6, 1625496362000L),
                new Tuple3<>("ss", 7, 1625496362000L),
                new Tuple3<>("se", 8, 1625496302000L),
                new Tuple3<>("se", 9, 1625496302000L),
                new Tuple3<>("se", 10, 1625496362000L),
                new Tuple3<>("ss", 11, 1625496422000L),
                new Tuple3<>("ss", 12, 1625496422000L),
                new Tuple3<>("aa", 13, 1625496492000L),
                new Tuple3<>("aa", 14, 1625496492000L),
                new Tuple3<>("ss", 15, 1625496492000L),
                new Tuple3<>("ss", 16, 1625496492000L),
                new Tuple3<>("se", 13, 1625496362000L)
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<String, Integer, Long>> input1 = env.fromCollection(data);
        DataStreamSource<Tuple3<String, Integer, Long>> input2 = env.fromCollection(data);

        input1.join(input2)
                .where(new KeySelector<Tuple3<String, Integer, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple3<String, Integer, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple3<String, Integer, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple3<String, Integer, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Object>() {
                    @Override
                    public Object join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
                        return null;
                    }
                })
                .print();
    }
}
