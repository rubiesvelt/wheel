import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;


public class WindowAgg {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Integer, Long>> data = Arrays.asList(
                new Tuple3<>("ss", 1, 1625496302000L),
                new Tuple3<>("ss", 2, 1625496302000L),
                new Tuple3<>("aa", 4, 1625496302000L),
                new Tuple3<>("aa", 5, 1625496362000L),
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

        DataStreamSource<Tuple3<String, Integer, Long>> input = env.fromCollection(data);
        input.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2))
                .keyBy((KeySelector<Tuple3<String, Integer, Long>, Object>) value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 增量聚合
                .aggregate(new AggregateFunction<Tuple3<String, Integer, Long>, Tuple5<String, Integer, Integer, Integer, Integer>, Tuple4<String, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> createAccumulator() {
                        return Tuple5.of("", Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 0);
                    }

                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> add(Tuple3<String, Integer, Long> value, Tuple5<String, Integer, Integer, Integer, Integer> accumulator) {
                        accumulator.f0 = value.f0;  // 此时这里手动获取 key 值
                        accumulator.f1 = value.f1 > accumulator.f1 ? value.f1 : accumulator.f1;
                        accumulator.f2 = value.f1 < accumulator.f2 ? value.f1 : accumulator.f2;
                        accumulator.f3 += value.f1;
                        accumulator.f4++;
                        return accumulator;
                    }

                    @Override
                    public Tuple4<String, Integer, Integer, Integer> getResult(Tuple5<String, Integer, Integer, Integer, Integer> accumulator) {
                        return Tuple4.of(accumulator.f0, accumulator.f1, accumulator.f2, accumulator.f3 / accumulator.f4);
                    }

                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Integer> merge(Tuple5<String, Integer, Integer, Integer, Integer> a, Tuple5<String, Integer, Integer, Integer, Integer> b) {
                        int max = Math.max(a.f1, b.f1);
                        int min = Math.min(a.f2, b.f2);
                        int sum = a.f3 + b.f3;
                        int cnt = a.f4 + b.f4;
                        return Tuple5.of(a.f0, max, min, sum, cnt);
                    }
                })
//                // 全量聚合
//                .process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Integer>, Object, TimeWindow>() {
//                    @Override
//                    public void process(Object o, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Tuple4<String, Integer, Integer, Integer>> out) {
//                        Tuple4<String, Integer, Integer, Integer> t = Tuple4.of((String) o, Integer.MIN_VALUE, Integer.MAX_VALUE, -1);
//                        int cnt = 0;
//                        int sum = 0;
//                        for (Tuple3<String, Integer, Long> element : elements) {
//                            cnt++;
//                            sum += element.f1;
//                            if (element.f1 > t.f1) t.f1 = element.f1;
//                            if (element.f1 < t.f2) t.f2 = element.f1;
//                        }
//                        t.f3 = sum / cnt;
//                        out.collect(t);
//                    }
//                })
                .print();
        env.execute();
    }
}
