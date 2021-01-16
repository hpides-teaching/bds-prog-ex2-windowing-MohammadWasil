package com.github.hpides.windowing;

import java.util.List;

/**
 * This is the main class for the exercise. The job of the WindowAggregationOperator is to take in a stream of events,
 * group them into windows and perform aggregations on the data in those windows.
 *
 * The window types that need to be supported are:
 *   - TumblingWindow (time-basesd tumbling window)
 *   - SlidingWindow (time-based sliding window)
 *   - SessionWindow (time-based session window)
 *   - TumblingCountWindow (count-based tumbling window)
 *
 * The aggregation functions that need to be supported are:
 *   - SumAggregateFunction (sum aggregation)
 *   - AvgAggregateFunction (average aggregation)
 *   - MedianAggregateFunction (median aggregation)
 *
 * Read the individual documentation for the classes to understand them better. You should not have to change any code
 * in the aggregate or window classes.
 *
 * This class has two methods that need to be implemented. See the documentation below for more details on them.
 */
public class WindowAggregationOperator {

    private final Window window;
    private final AggregateFunction aggregateFunction;


    /**
     * This constructor is called to create a new WindowAggregationOperator with a given window type and aggregation
     * function. You will possibly need to extend this to initialize some variables but do not change the signature.
     *
     * @param window The window type that should be used in the windowed aggregation.
     * @param aggregateFunction The aggregation function that should be used in the windowed aggregation.
     */
    public WindowAggregationOperator(final Window window, final AggregateFunction aggregateFunction) {
        this.window = window;
        this.aggregateFunction = aggregateFunction;

        // YOUR CODE HERE
        //
        // YOUR CODE HERE END
    }

    /**
     * This method is a key method for the streaming operator. It takes in an Event and performs the necessary
     * computation for a windowed aggregation. You should implement the logic here to support the window types and
     * aggregation functions mentioned above. The order of the events based on their timestamps is not guaranteed,
     * i.e., they can arrive out-of-order. You should account for this in the more advanced test cases
     * (see OutOfOrderStreamTest) and our hidden test cases.
     *
     * @param event The event that should be processed.
     */
    public void processEvent(final Event event) {
        // YOUR CODE HERE
        //
        // YOUR CODE HERE END
        private final long value;
        private final long timestamp;
        private final long length;
        //private final SumAggregateFunction sumFn;
        private final long sum;
        // Get the value of the timestamp and the value
        this.value = event.getValue();
        this.timestamp = event.getTimestamp();
        
        this.length = this.window.getLength();
        
        // maybe if else to check whether tumbling window  or other window, nested ifelse for type of aggregation function.
        // Lets say, we are doing sum aggregation.
        //final AggregateFunction sumFn = new SumAggregateFunction();
	
	this.sum = this.aggregateFunction.aggregate(this.value);

    }

    /**
     * This method triggers the complete windows up to the watermark. Remember, a watermark is a special event that
     * tells the operators that no events with a lower timestamp that the watermark will arrive in the future. If an
     * older event arrives, it should be ignored because the result has already been emitted.
     *
     * This method is responsible for producing the aggregated output values for the specified windows. As a watermark
     * can trigger multiple previous windows, the results are returned as a list. Read the ResultWindow documentation
     * for more details on the fields that it has and what they mean for different window types. The order of the
     * ResultWindows is determined by the endTime, earliest first.
     *
     * In this assignment, we will not use watermarks that are far in the future and cause many "empty" windows. A
     * watermark will only complete a window i) that has events in it or ii) that is empty but newer events have created
     * a newer window.
     *
     * As the endTime of a window is excluded from the window's events, a watermark at time 15 can cause a window ending
     * at 15 to be triggered.
     *
     * @param watermarkTimestamp The event-time timestamp of the watermark.
     * @return List of ResultWindows for all the windows that are complete based on the knowledge of the watermark.
     */
    public List<ResultWindow> processWatermark(final long watermarkTimestamp) {
        // YOUR CODE HERE
        final List<ResultWindow> results = new ResultWindow();

        for(int i =0; i < 3 ; i++)
        {
		// update results
		results[i] = new ResultWindow( i*this.length, (i+1)*this.length, this.sum);
        }
        
        return results;
        // YOUR CODE HERE END
    }
}
