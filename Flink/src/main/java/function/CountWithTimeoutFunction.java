package function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.CountWithTimestamp;

import java.util.Arrays;
import java.util.List;

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
public class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

    public static void main(String[] args) {
        // the source data stream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, String>> data = Arrays.asList(
                new Tuple2<>("ss", "1625496302000"),
                new Tuple2<>("ss", "1625496302000"),
                new Tuple2<>("aa", "1625496302000"),
                new Tuple2<>("aa", "1625496362000"),
                new Tuple2<>("ss", "1625496362000"),
                new Tuple2<>("ss", "1625496362000"),
                new Tuple2<>("se", "1625496302000"),
                new Tuple2<>("se", "1625496302000")
        );
        DataStream<Tuple2<String, String>> stream = env.fromCollection(data);

        // apply the process function onto a keyed stream

        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(value -> value.f0)
                .process(new CountWithTimeoutFunction());
    }

    /**
     * The state that is maintained by this process function
     */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            Tuple2<String, String> value,
            Context ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}