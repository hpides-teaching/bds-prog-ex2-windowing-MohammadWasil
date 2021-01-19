package com.github.hpides.windowing;

import java.util.List;
import java.security.Timestamp;
import java.util.*;

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
    
    private HashMap<Long, Long> inOrderStreamTestSumHash = new HashMap<Long, Long>();
    private long timeStamp;
    private long value;

    private long sum;
    private long startTime;
    private long endTime;

    // Temporary variable for sliding window
    private long length = 10L;
    private long slide = 5L;
    //private List<Long> slidingWindow = Arrays.asList(new Long[10]);
    private HashMap<Long, Long> slidingWindow = new HashMap<Long, Long>();
    
    //public ResultWindow resultWindow; // = new ResultWindow(1L, 1L, 1L);
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
	    //System.out.println("Process Event");
        // YOUR CODE HERE
        //
        // YOUR CODE HERE END
        if(this.window.getClass().getSimpleName().equals("TumblingWindow"))
        {
            // Tumbling window -> Sum Aggreagtion function
            if(this.aggregateFunction.getClass().getSimpleName().equals("SumAggregateFunction") ||
            this.aggregateFunction.getClass().getSimpleName().equals("AvgAggregateFunction") ||
            this.aggregateFunction.getClass().getSimpleName().equals("MedianAggregateFunction") )
            {
                timeStamp = event.getTimestamp();
                value     = event.getValue();
                // Store the timestamp-value in a hashmap.
                if( inOrderStreamTestSumHash.containsKey(timeStamp))
                {
                    inOrderStreamTestSumHash.put(timeStamp, value);
                }
                else
                {
                    inOrderStreamTestSumHash.putIfAbsent(timeStamp, value);		
                }
            }
        }

        // For sliding window
        /*if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            // Tumbling window -> Sum Aggreagtion function
            //if(this.aggregateFunction.getClass().getSimpleName().equals("SumAggregateFunction") ||
            //this.aggregateFunction.getClass().getSimpleName().equals("AvgAggregateFunction") ||
            //this.aggregateFunction.getClass().getSimpleName().equals("MedianAggregateFunction") )
            //{
                timeStamp = event.getTimestamp();
                value     = event.getValue();
                // Store the timestamp-value in a hashmap.
                for(long i = 0; i <= 10; i+=5)
                {
                    //System.out.println( i + " " + (length+i));
                    if((timeStamp >= i) && (timeStamp < (length+i)) )
                    {
                        // check if the lentgh if the list is smaller than the given timestamp
                        //if( (int)(timeStamp) < slidingWindow.size() )
                        //{
                            //slidingWindow.set((int)(timeStamp), value);
                            if( slidingWindow.containsKey(timeStamp))
                            {
                                slidingWindow.put(timeStamp, value);
                            }
                            else
                            {
                                slidingWindow.putIfAbsent(timeStamp, value);		
                            }
                            System.out.println(slidingWindow);
                        //}
                    }
                }
        */
                
                //System.out.println(inOrderStreamTestSumHash);
                /*for(long i = 0; i <= 10; i+=5)
                {
                    //System.out.println( i + " " + (length+i));
                    if((timeStamp >= i) && (timeStamp < (length+i)) )
                    {
                        // check if the lentgh if the list is smaller than the given timestamp
                        if( (int)(timeStamp) < slidingWindow.size() )
                        {
                            slidingWindow.set((int)(timeStamp), value);
                        }
                    }
                }
                // convert the remianing null values as 0.
                for(int x = 0; x < slidingWindow.size(); x++)
                {
                    if(slidingWindow.get(x) == null)
                    {
                        slidingWindow.set(x, 0L);
                    }
                }
                System.out.println(slidingWindow);
                */
            //}
        //}
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
        //System.out.println("Process watermark");
        // YOUR CODE HERE
        // Iterate over the hasmap, and then check if the given timestamp is under the watermark stamp
        final List<ResultWindow> resultList = new ArrayList<ResultWindow>();
        
        if(this.window.getClass().getSimpleName().equals("TumblingWindow"))
        {
            List<Long> inOrderStreamTestSum = new ArrayList<Long>();
            for(Map.Entry<Long, Long> e: inOrderStreamTestSumHash.entrySet())
            {
                if(e.getKey() <= watermarkTimestamp)
                {
                    inOrderStreamTestSum.add(e.getValue());
                    //System.out.println(inOrderStreamTestSum);
                    // do the aggregation here. 	
                    sum = this.aggregateFunction.aggregate(inOrderStreamTestSum);
                    
                    timeStamp = e.getKey();
                    if( (10L % timeStamp) != 10L) // start from 0
                    {
                        startTime = 0L;
                        endTime = startTime + 10L; // 10L is the length of the window.
                    }
                    else{               // start from next 10 time stamp.
                        startTime = (long)((int)(timeStamp/10) *10 );
                        endTime = startTime + 10L; // 10L is the length of the window.
                    }
                    
                }
            }
        }

        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            for(Map.Entry<Long, Long> e: slidingWindow.entrySet())
            {
                if(e.getKey() <= watermarkTimestamp)
                {
                    List<Long> inOrderStreamTestSum = new ArrayList<Long>();
                    inOrderStreamTestSum.add(e.getValue());
                    System.out.println(inOrderStreamTestSum);
                    // do the aggregation here. 	
                    sum = this.aggregateFunction.aggregate(inOrderStreamTestSum);
                    endTime = e.getKey();
                    startTime = 0L;
                }
            }
        }
        
        ResultWindow r = new ResultWindow(startTime, endTime, sum);
        //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
        resultList.add(r);
        return resultList;
         
        // YOUR CODE HERE END
    }
}
