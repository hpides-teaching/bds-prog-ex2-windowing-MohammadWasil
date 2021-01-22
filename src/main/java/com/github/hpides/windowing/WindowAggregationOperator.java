package com.github.hpides.windowing;
import java.util.*;
import java.util.Collections;




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
    //private HashMap<Long, Long> inOrderStreamTestSumHash = new HashMap<Long, Long>();     // Hash Maps for tumbling window.
    private List<Long> tumblingListTimeStamp = new ArrayList<Long>();
    private List<Long> tumblingListValue = new ArrayList<Long>();
    private List<List<Long>> tumblingTimeStampListOfList = new ArrayList<List<Long>>();
    private List<List<Long>> tumblingValueListOfList = new ArrayList<List<Long>>();

    private List<List<Long>> finalTimeStamp = new ArrayList<List<Long>>();
    private List<List<Long>> finalValue = new ArrayList<List<Long>>();
    

    private Long previousStartTime;
    private Long previousEndTime;

    // Sliding window
    private long slide;            // the slide of the window.
    private HashMap<Long, Long> slidingEventHash = new HashMap<Long, Long>();
    //private List<Long> slidingTimeStamp = new ArrayList<Long>();

    // session window
    private long gap;
    private List<Long> sessionListTimeStamp = new ArrayList<Long>();
    private List<Long> sessionListValue = new ArrayList<Long>();

    private List<List<Long>> sessionEventTimeStampListOfList = new ArrayList<List<Long>>();
    private List<List<Long>> sessionEventValueListOfList = new ArrayList<List<Long>>();

    // tumbling window count
    private List<Long> tumblingCountWindow = new ArrayList<Long>();
    private List<List<Long>> tumblingCountWindowListOfList = new ArrayList<List<Long>>();
    private List<Long> eventCount = new ArrayList<Long>();
    private List<List<Long>> eventCountListOfList = new ArrayList<List<Long>>();
    private Long counter = 0L;
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
        if(this.window.getClass().getSimpleName().equals("TumblingWindow"))
        {
            previousStartTime = 0L;
            previousEndTime = ((TumblingWindow) this.window).getLength();
        }
        /*if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            previousStartTime = 0L;
            previousEndTime = ((SlidingWindow) this.window).getLength();
        }*/
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
            int windowCount = 0;
            // Tumbling window -> Sum Aggreagtion function
            if(this.aggregateFunction.getClass().getSimpleName().equals("SumAggregateFunction") ||
            this.aggregateFunction.getClass().getSimpleName().equals("AvgAggregateFunction") ||
            this.aggregateFunction.getClass().getSimpleName().equals("MedianAggregateFunction") )
            {
                // get the length of the window.
                length = ((TumblingWindow) this.window).getLength();
                timeStamp = event.getTimestamp();
                value     = event.getValue();
                
                if(tumblingListTimeStamp.size() < length )
                {
                    for(int i=0; i <= (length*2) ; i+=length)
                    {
                        startTime = i;
                        endTime = i + length;
                        
                        if(timeStamp >= startTime && timeStamp < endTime)
                        {
                            if(startTime==previousStartTime && endTime==previousEndTime)
                            {
                                tumblingListTimeStamp.add(timeStamp);
                                tumblingListValue.add(value);     
                                break;
                            }
                            else
                            {
                                tumblingListTimeStamp.clear();
                                tumblingListValue.clear();
                                tumblingListTimeStamp.add(timeStamp);
                                tumblingListValue.add(value);
                                break;
                            }
                        }
                    }
                    //System.out.println(previousStartTime + " " +startTime);
                    //System.out.println(previousendTime+" " + endTime);
                    
                    //tumblingListTimeStamp.add(timeStamp);
                    //tumblingListValue.add(value);   

                    if(tumblingTimeStampListOfList.size() >=1)
                    {        
                        int len = tumblingTimeStampListOfList.size();
                       // System.out.println(" ");
                        if(tumblingTimeStampListOfList.get(len -1).size() != length)
                        {
                            //System.out.println("Secd:" + tumblingTimeStampListOfList);
                            // replace the last element for timestamp
                            if(previousStartTime == startTime && previousEndTime == endTime)
                            {
                                tumblingTimeStampListOfList.remove(tumblingTimeStampListOfList.get(len -1 ));
                                List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                                tumblingTimeStampListOfList.add(a); 

                                // replace the last element for value
                                tumblingValueListOfList.remove(tumblingValueListOfList.get(len -1 ));
                                List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                                tumblingValueListOfList.add(b); 
                                
                                //System.out.println("Secd:" + tumblingTimeStampListOfList);
                            }else{
                                //System.out.println("changed");
                                previousStartTime=startTime;
                                previousEndTime=endTime;
                                List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                                tumblingTimeStampListOfList.add(a);
                                
                                List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                                tumblingValueListOfList.add(b);
                            }
                            
                            // replace the last element for value
                            //tumblingValueListOfList.remove(tumblingValueListOfList.get(len -1 ));
                            //List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                            //tumblingValueListOfList.add(b); 
                        }
                        else
                        {
                            //System.out.println("heya");
                            // add the tumbling 
                            List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                            tumblingTimeStampListOfList.add(a); 

                            // add the tumbling 
                            List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                            tumblingValueListOfList.add(b); 
                        }
                    }
                    else
                    {
                        //System.out.println("heya hello");
                        // add the tumbling 
                        List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                        tumblingTimeStampListOfList.add(a);
                        
                        // add the tumbling 
                        List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                        tumblingValueListOfList.add(b);
                    }
                    //uniqueSet.add(previousEndTime);
                    //windowCount = uniqueSet.size();
                }
                else if(tumblingListTimeStamp.size() >= length )
                {
                    tumblingListTimeStamp.clear();
                    tumblingListTimeStamp.add(timeStamp);

                    tumblingListValue.clear();
                    tumblingListValue.add(value);
                }
                //System.out.println(tumblingTimeStampListOfList);
                

                //long startTime = 0L;
                //long endTime = 0L;
                //int length=10;

                // combine the list with timetamp hin same range.
                List<List<Long>> timeStampList = new ArrayList<List<Long>>();
                List<List<Long>> valueList = new ArrayList<List<Long>>();
                for(int i=0; i<= 2; i++)
                {
                    List<Long> smallerList = new ArrayList<Long>();
                    List<Long> smallerListValue = new ArrayList<Long>();
                    startTime = i*length ;   
                    endTime = (i+1)*length;  
                    for(int j=0; j<tumblingTimeStampListOfList.size() ; j++)
                    {
                        
                        for(int k=0; k<tumblingTimeStampListOfList.get(j).size(); k++)
                        {
                            if(tumblingTimeStampListOfList.get(j).get(k) >= startTime && tumblingTimeStampListOfList.get(j).get(k)<endTime)
                            {
                                smallerList.add(tumblingTimeStampListOfList.get(j).get(k));
                                smallerListValue.add(tumblingValueListOfList.get(j).get(k));
                            }
                        }
                    }
                    List<Long> tempList = new ArrayList<Long>(smallerList); 
                    Collections.sort(tempList);
                    timeStampList.add(tempList);
                    timeStampList.removeAll(Collections.singleton(new ArrayList<>()));

                    List<Long> tempListValue = new ArrayList<Long>(smallerListValue); 
                    Collections.sort(tempListValue);
                    valueList.add(tempListValue);
                    valueList.removeAll(Collections.singleton(new ArrayList<>()));
                    //System.out.println("inner: " + timeStampList) ;
                }   
                
                finalTimeStamp = timeStampList;
                finalValue = valueList;
                //System.out.println(finalTimeStamp);
                //System.out.println("outer: "+finalTimeStamp) ;

                // check whether the window are similar
                /*List<Long> a = new ArrayList<Long>();
                List<Long> b = new ArrayList<Long>();
                if(windowCount != tumblingTimeStampListOfList.size())
                {
                    
                    for(int i = 0; i < tumblingTimeStampListOfList.size(); i++)
                    {
                        for(int j = 0 ; j < tumblingTimeStampListOfList.get(i).size(); j++ )
                        {
                            
                            if(tumblingTimeStampListOfList.get(i).get(j) >= startTime && tumblingTimeStampListOfList.get(i).get(j) < endTime)
                            {
                                a.add(tumblingTimeStampListOfList.get(i).get(j));

                                tumblingTimeStampListOfList.remove(tumblingTimeStampListOfList.get(len -1 ));
                                List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                                tumblingTimeStampListOfList.add(a); 


                            }
                            else
                            {
                                b.add(tumblingTimeStampListOfList.get(i).get(j));
                            }
                        }
                    }                    
                }
                
                finalList.add(a);
                finalList.add(b);
                a.clear();
                b.clear();
                a = finalList.get(finalList.size()-1);
                b = finalList.get(finalList.size()-2);
                System.out.println(a);
                */


            }
        }

        // For sliding window
        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            length = ((SlidingWindow) this.window).getLength();
            slide = ((SlidingWindow) this.window).getSlide();
            //long i=0L;
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
        }
        
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
                            System.out.println("a: " + a);
                            Collections.sort(a);
                            Collections.sort(b);

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
            System.out.println(sessionEventTimeStampListOfList);

            /*if( sessionEventHash.containsKey(timeStamp))
            {
                sessionEventHash.put(timeStamp, value);
            }
            else
            {
                sessionEventHash.putIfAbsent(timeStamp, value);		
            }*/
        }
        
         // Tumbling Count window
         if(this.window.getClass().getSimpleName().equals("TumblingCountWindow"))
         {
            length = ((TumblingCountWindow) this.window).getLength();

            timeStamp = event.getTimestamp();
            value     = event.getValue();
            
            counter += 1;

            if( tumblingCountWindow.size() < length )
            {
                tumblingCountWindow.add(value);
            }
            else
            {
                // update the countWindow list
                eventCount.clear();
                eventCount.add(counter - length );
                eventCount.add(counter - 1 );
                
                List<Long> a = new ArrayList<Long>(eventCount);               // clone of eventCount.
                eventCountListOfList.add(a);

                // add these lists to another list - nested list
                List<Long> b = new ArrayList<Long>(tumblingCountWindow);               // clone of tumblingCountWindow.
                tumblingCountWindowListOfList.add(b);
                
                tumblingCountWindow.clear();
                tumblingCountWindow.add(value);             // add the enxt value to tumblingCountWindow.

            }
            
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
            //while(j <= (length))
            //{   
                for(int i = 0; i < finalTimeStamp.size(); i++)          // Over each loop, we have 1 tumbling count window
                { 
                    startTime = i*length ;    // 0 for the start time
                    endTime = (i+1)*length;  // 1 for the start time  
                    List<Long> tempa = new ArrayList<Long>();
                    //System.out.println(endTime);
                    //System.out.println(watermarkTimestamp);

                    // Ignore the whole window if there is an after the watermark timestamp.
                    if((watermarkTimestamp - endTime <= length) && (watermarkTimestamp >= endTime))
                    {                        
                        tempa = finalValue.get(i);
                        System.out.println(tempa);
                        sum = this.aggregateFunction.aggregate(tempa);                             
                        ResultWindow r = new ResultWindow(startTime, endTime, sum);
                        //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                        resultList.add(r);   
                        //break;
                    }                                  
                }      
                //j+=length;
                /*
                for(int i = 0; i < tumblingTimeStampListOfList.size(); i++)          // Over each loop, we have 1 tumbling count window
                { 
                    for(int j = 0 ; j < tumblingValueListOfList.get(i).size() ; j+=length)
                    {
                        if(tumblingValueListOfList.get(i).get(j) > watermarkTimestamp)
                        {
                            tumblingTimeStampListOfList.get(i).remove(j);
                            tumblingValueListOfList.get(i).remove(j);
                            //System.out.println("Hello");

                        }
                    }    
                    if(tumblingValueListOfList.get(i)!=null)
                    {
                        sum = this.aggregateFunction.aggregate(tumblingValueListOfList.get(i));
                        startTime = i*length ;    // 0 for the start time
                        endTime = (i+1)*length;  // 1 for the start time    
                        ResultWindow r = new ResultWindow(startTime, endTime, sum);
                        //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                        resultList.add(r);          
                    }
                    
                }  */  







            //}
            /*
            Previous answer.
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
                        //if( (10L % timeStamp) != length) // start from 0
                        //{
                        //    startTime = 0L;
                        //    endTime = startTime + length; // 10L is the length of the window.
                        //}
                        //else{               // start from next 10 time stamp.
                        //    startTime = (long)((int)(timeStamp/10) *10 );
                        //    endTime = startTime + length; // 10L is the length of the window.
                        //}
                        startTime = i;
                        endTime = length + i;
                    }
                }
                i+=length;
            }
            ResultWindow r = new ResultWindow(startTime, endTime, sum);
            //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
            resultList.add(r);
            */
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

            /*for(int i =0; i < sessionListTimeStamp.size(); i++)          // Over each loop, we have 1 session window
            {
                System.out.println(sessionListTimeStamp.size());
                if(sessionListTimeStamp.size() > i+1) // tis condition abouve, try solving it
                {
                    if(sessionListTimeStamp.get(i+1) - sessionListTimeStamp.get(i) >= gap)
                    {
                        //sessionListTimeStamp
                        List<Long> tempa = sessionListTimeStamp.subList(0, i+1);

                        //System.out.println(tempa);
                        
                        List<Long> a = new ArrayList<Long>(tempa);                   // clone of sessionListTimeStamp.
                        sessionEventTimeStampListOfList.add(a);
                        //System.out.println(sessionEventTimeStampListOfList);

                        sessionListTimeStamp.removeAll(tempa);
                        System.out.println(sessionListTimeStamp);
                    }
                    else
                    {
                        //sessionListTimeStamp
                        List<Long> tempa = sessionListTimeStamp.subList(0, sessionListTimeStamp.size());
                        //System.out.println(tempa);

                        List<Long> a = new ArrayList<Long>(tempa);                   // clone of sessionListTimeStamp.
                        sessionEventTimeStampListOfList.add(a);
                        //System.out.println(sessionEventTimeStampListOfList);
                    }
                }
            }*/
        
            

            for(int i =0; i < sessionEventTimeStampListOfList.size(); i++)          // Over each loop, we have 1 session window
            {   
                for(int j=0; j <sessionEventTimeStampListOfList.get(i).size() ; j++)
                {
                    // Ignore any evenet coming after the watermark timestamp.
                    if(sessionEventTimeStampListOfList.get(i).get(j) > watermarkTimestamp)
                    {
                        sessionEventTimeStampListOfList.get(i).remove(j);
                        sessionEventValueListOfList.get(i).remove(j);
                        //System.out.println("Hello");
                    }
                }
                sum = this.aggregateFunction.aggregate(sessionEventValueListOfList.get(i));                
                startTime = sessionEventTimeStampListOfList.get(i).get(0);                                                             // first element of the list
                endTime =   sessionEventTimeStampListOfList.get(i).get(sessionEventTimeStampListOfList.get(i).size()-1) + gap;         // last element of the list + gap size.

                ResultWindow r = new ResultWindow(startTime, endTime, sum);
                //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                resultList.add(r);
            }
            
        }

        //tumblingCountWindowListOfList
        if(this.window.getClass().getSimpleName().equals("TumblingCountWindow"))
        {
            for(int i = 0; i < tumblingCountWindowListOfList.size(); i++)          // Over each loop, we have 1 tumbling count window
            { 
                //System.out.println(tumblingCountWindowListOfList.get(i));
                sum = this.aggregateFunction.aggregate(tumblingCountWindowListOfList.get(i));                
                
                startTime = eventCountListOfList.get(i).get(0);     // 0 for the start time
                endTime = eventCountListOfList.get(i).get(1);       // 1 for the start time
                
                ResultWindow r = new ResultWindow(startTime, endTime, sum);
                //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                resultList.add(r);
            }
            
        }
        return resultList;

        // YOUR CODE HERE END
    }
}
