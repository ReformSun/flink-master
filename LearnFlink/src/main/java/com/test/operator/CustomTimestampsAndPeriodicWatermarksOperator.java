package com.test.operator;

import com.test.alarm.AssignerElement;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

public class CustomTimestampsAndPeriodicWatermarksOperator<T>
	extends AbstractUdfStreamOperator<T, AssignerWithPeriodicWatermarks<T>>
	implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {
	private static final long serialVersionUID = 1L;

	private transient long watermarkInterval;

	private transient long currentWatermark;
	private final AssignerElement<T> assignerElement;

	public CustomTimestampsAndPeriodicWatermarksOperator(AssignerWithPeriodicWatermarks<T> assigner, AssignerElement<T> assignerElement) {
		super(assigner);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
		this.assignerElement = assignerElement;
	}

	@Override
	public void open() throws Exception {
		super.open();

		currentWatermark = Long.MIN_VALUE;
		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);

			ProcessingTimeCallback processingTimeCallback = new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					StreamRecord<T> streamRecord = new StreamRecord<T>(assignerElement.getElement(),System.currentTimeMillis());
					processElement(streamRecord);
					long now = getProcessingTimeService().getCurrentProcessingTime();
					getProcessingTimeService().registerTimer(now + 60000, this);
				}
			};
			getProcessingTimeService().registerTimer(now + watermarkInterval, processingTimeCallback);
		}
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
			element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		output.collect(element.replace(element.getValue(), newTimestamp));
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		// register next timer
		Watermark newWatermark = userFunction.getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = newWatermark.getTimestamp();
			// emit watermark
			output.emitWatermark(newWatermark);
		}

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPeriodicWatermarks} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final watermark
		Watermark newWatermark = userFunction.getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = newWatermark.getTimestamp();
			// emit watermark
			output.emitWatermark(newWatermark);
		}
	}
}
