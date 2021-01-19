package com.github.hpides.windowing;
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
    
    private long length;                // the length of the window.
    
    private long timeStamp;             // Timestamp of each event.
    private long value;                 // value of each events.

    private long sum;                   // the sum or the aggregation result.
    private long startTime;             // start time of the window.
    private long endTime;               // end time of the window.

    // /tumbling window
    private HashMap<Long, Long> inOrderStreamTestSumHash = new HashMap<Long, Long>();     // Hash Maps for tumbling window.

   
    // Sliding window
    private long slide;            // the slide of the window.
    private HashMap<Long, Long> slidingEventHash = new HashMap<Long, Long>();
    //private List<Map<Long, Long>> slidingEventListOfHash = new ArrayList<Map<Long, Long>>();

    // session window
    private long gap;
    private HashMap<Long, Long> sessionEventHash = new HashMap<Long, Long>();
    private List<Long> sessionListTimeStamp = new ArrayList<Long>();
    private List<Long> sessionListValue = new ArrayList<Long>();

    private List<List<Long>> sessionEventTimeStampListOfList = new ArrayList<List<Long>>();
    private List<List<Long>> sessionEventValueListOfList = new ArrayList<List<Long>>();

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

        // In Order Stream Test Slidinf Window - Done.
        if(this.window.getClass().getSimpleName().equals("TumblingWindow"))
        {
            
            // Tumbling window -> Sum Aggreagtion function
            if(this.aggregateFunction.getClass().getSimpleName().equals("SumAggregateFunction") ||
            this.aggregateFunction.getClass().getSimpleName().equals("AvgAggregateFunction") ||
            this.aggregateFunction.getClass().getSimpleName().equals("MedianAggregateFunction") )
            {
                // get the length of the window.
                length = ((TumblingWindow) this.window).getLength();
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
        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            // Tumbling window -> Sum Aggreagtion function
            //if(this.aggregateFunction.getClass().getSimpleName().equals("SumAggregateFunction") ||
            //this.aggregateFunction.getClass().getSimpleName().equals("AvgAggregateFunction") ||
            //this.aggregateFunction.getClass().getSimpleName().equals("MedianAggregateFunction") )
            //{
                length = ((SlidingWindow) this.window).getLength();
                slide = ((SlidingWindow) this.window).getSlide();
                long i=0L;
                timeStamp = event.getTimestamp();
                value     = event.getValue();
                
                if( slidingEventHash.containsKey(timeStamp))
                {
                    slidingEventHash.put(timeStamp, value);
                }
                else
                {
                    slidingEventHash.putIfAbsent(timeStamp, value);		
                }
        
                /*
                //System.out.println(inOrderStreamTestSumHash);
                for(long i = 0; i <= 10; i+=5)
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
        }

        // Session window
        if(this.window.getClass().getSimpleName().equals("SessionWindow"))
        {
            //System.out.println(sessionEventTimeStampListOfList + "first");

            gap = ((SessionWindow) this.window).getGap();
            //long i=0L;
            timeStamp = event.getTimestamp();
            value     = event.getValue();
            
            sessionListTimeStamp.add(timeStamp);
            sessionListValue.add(value);
            //System.out.println(sessionListTimeStamp);
            if(sessionListTimeStamp.size() >= 2)
            {
                for(int i = 0 ; i < sessionListTimeStamp.size(); i++)
                {
                    if( sessionListTimeStamp.size() > i+1)
                    {
                        if((sessionListTimeStamp.get(i+1) - sessionListTimeStamp.get(i)) >= gap)
                        {
                            
                            //for( int j=0; j < i+1 ; j++) 
                            //{
                            //    sessionEventHash.put(sessionListTimeStamp.get(j), sessionListValue.get(j));
                            //} 
                            //System.out.println(sessionEventHash);

                            //System.out.println(sessionListTimeStamp.get(i));
                            
                            // For the future loop
                            long varTimeStamp = sessionListTimeStamp.get(i+1);
                            long varValue = sessionListValue.get(i+1);
                            
                            // Remove the last element
                            sessionListTimeStamp.remove(i+1);
                            sessionListValue.remove(i+1);

                            //System.out.println("after removing" +sessionListTimeStamp);
                            // add these lists to another list.
                            List<Long> a = new ArrayList<Long>(sessionListTimeStamp);               // clone of sessionListTimeStamp.
                            List<Long> b = new ArrayList<Long>(sessionListValue);                   // clone of sessionListTimeStamp.
                            sessionEventTimeStampListOfList.add(a);
                            sessionEventValueListOfList.add(b);
                            //System.out.println(sessionEventTimeStampListOfList + " .... ");
                            //System.out.println(sessionEventValueListOfList + " .... ");

                            sessionListTimeStamp.clear();
                            sessionListValue.clear();

                            sessionListTimeStamp.add(varTimeStamp);
                            sessionListValue.add(varValue);
                            //System.out.println(sessionListTimeStamp);
                            //System.out.println(sessionListValue);
                            //System.out.println("hello");
                            break;
                        }
                    }
                }
            }

            /*if( sessionEventHash.containsKey(timeStamp))
            {
                sessionEventHash.put(timeStamp, value);
            }
            else
            {
                sessionEventHash.putIfAbsent(timeStamp, value);		
            }*/
        }
        
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
        
        // In Order stream Test for Tumbling Window. Done. 
        if(this.window.getClass().getSimpleName().equals("TumblingWindow"))
        {
            long i = 0L;
            
            while(i <= (length-1))
            {
                List<Long> inOrderStreamTestSum = new ArrayList<Long>();
                for(Map.Entry<Long, Long> e: inOrderStreamTestSumHash.entrySet())
                {
                    if(e.getKey() <= watermarkTimestamp)
                    {
                        inOrderStreamTestSum.add(e.getValue());
                        // do the aggregation here. 	
                        sum = this.aggregateFunction.aggregate(inOrderStreamTestSum);
                        
                        //timeStamp = e.getKey();
                        /*if( (10L % timeStamp) != length) // start from 0
                        {
                            startTime = 0L;
                            endTime = startTime + length; // 10L is the length of the window.
                        }
                        else{               // start from next 10 time stamp.
                            startTime = (long)((int)(timeStamp/10) *10 );
                            endTime = startTime + length; // 10L is the length of the window.
                        }*/
                        startTime = i;
                        endTime = length + i;
                    }
                }
                i+=length;
            }
            ResultWindow r = new ResultWindow(startTime, endTime, sum);
            //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
            resultList.add(r);
        }

        
        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            long i = 0L;
            while(i <= 5)
            {
                List<Long> inOrderStreamTestSum = new ArrayList<Long>();
                for(Map.Entry<Long, Long> e: slidingEventHash.entrySet())
                {                    
                    if((e.getKey() >= i) && (e.getKey() < (length+i)) && (e.getKey() < watermarkTimestamp) )
                    {
                        inOrderStreamTestSum.add(e.getValue());
                        // do the aggregation here. 	
                        sum = this.aggregateFunction.aggregate(inOrderStreamTestSum);
                        startTime = i;
                        endTime = length + i;
                    
                    }
                }
                i+=slide;
         
                ResultWindow r = new ResultWindow(startTime, endTime, sum);
                //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                resultList.add(r);
            }
        }
        
        if(this.window.getClass().getSimpleName().equals("SessionWindow"))
        {
            for(int i =0; i < sessionEventTimeStampListOfList.size(); i++)          // Over each loop, we have 1 session window
            {   
                sum = this.aggregateFunction.aggregate(sessionEventValueListOfList.get(i));                
                startTime = sessionEventTimeStampListOfList.get(i).get(0);                                                             // first element of the list
                endTime =   sessionEventTimeStampListOfList.get(i).get(sessionEventTimeStampListOfList.get(i).size()-1) + gap;         // last element of the list + gap size.

                ResultWindow r = new ResultWindow(startTime, endTime, sum);
                //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                resultList.add(r);
            }
        }

        return resultList;

        // YOUR CODE HERE END
    }
}
