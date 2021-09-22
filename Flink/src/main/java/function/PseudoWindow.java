package function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.TaxiFare;

/**
 * A demo of KeyedProcessFunction
 */
public class PseudoWindow extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    private final long durationMsec;

    // 每个窗口都持有托管的 Keyed state 的入口，并且根据窗口的结束时间执行 keyed 策略
    // 每个司机都有一个单独的 MapState 对象
    private transient MapState<Long, Float> sumOfTips;  // 窗口结束时间 -> 该时间段 tips 的和

    public PseudoWindow(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    public void open(Configuration conf) {
        MapStateDescriptor<Long, Float> sumDesc = new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
        sumOfTips = getRuntimeContext().getMapState(sumDesc);
    }

    @Override
    public void processElement(
            TaxiFare fare,
            KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>>.Context context,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        long eventTime = fare.getEventTime();
        TimerService timerService = context.timerService();  // 通过 context 获取 TimerService

        if (eventTime <= timerService.currentWatermark()) {
            // 事件时间，小于watermark，则丢弃，do nothing
        } else {
            // 将 eventTime 向上取值并将结果赋值到包含当前事件的窗口的末尾时间点
            long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

            // 在窗口完成时将启用回调
            timerService.registerEventTimeTimer(endOfWindow);  // 在 TimerService 中注册回调。每个元素都注册一次... 那岂不是要重复注册很多次

            // 将此票价的小费添加到该窗口的总计中
            Float sum = sumOfTips.get(endOfWindow);
            if (sum == null) {
                sum = 0.0F;
            }
            sum += fare.tip;
            sumOfTips.put(endOfWindow, sum);
        }
    }

    // 在 Timer 触发时回调此函数
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        long driverId = context.getCurrentKey();  // 通过 context 获取当前 key

        // 查找刚结束的一小时结果
        Float sumOfTips = this.sumOfTips.get(timestamp);

        Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
        out.collect(result);

        this.sumOfTips.remove(timestamp);
    }
}
