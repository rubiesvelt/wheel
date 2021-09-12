import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) throws Exception {
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
        DataStreamSource<Tuple3<String, Integer, Long>> input = env.fromCollection(data);
        DataStreamSource<Tuple3<String, Integer, Long>> input1 = env.fromCollection(data);

        input.flatMap(new FlatMapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tuple3<String, Integer, Long> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] strings = value.f0.split(" ");
                        for (String s : strings) {
                            out.collect(Tuple2.of(s, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();
        env.execute();
    }
}
