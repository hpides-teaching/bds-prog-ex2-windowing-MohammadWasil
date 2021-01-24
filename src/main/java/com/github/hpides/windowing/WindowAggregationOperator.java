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

    private Long previousStartTime;
    private Long previousEndTime;

    // /tumbling window
    //private HashMap<Long, Long> inOrderStreamTestSumHash = new HashMap<Long, Long>();     // Hash Maps for tumbling window.
    private List<Long> tumblingListTimeStamp = new ArrayList<Long>();
    private List<Long> tumblingListValue = new ArrayList<Long>();
    private List<List<Long>> tumblingTimeStampListOfList = new ArrayList<List<Long>>();
    private List<List<Long>> tumblingValueListOfList = new ArrayList<List<Long>>();

    private List<List<Long>> finalTimeStamp = new ArrayList<List<Long>>();
    private List<List<Long>> finalValue = new ArrayList<List<Long>>();

    // Sliding window
    private long slide;            // the slide of the window.
    private HashMap<Long, Long> slidingEventHash = new HashMap<Long, Long>();
    private List<Long> slidingTimeStamp = new ArrayList<Long>();
    private List<Long> slidingValue = new ArrayList<Long>();
    
    private List<List<Long>> slidingTimeStampListOfList = new ArrayList<List<Long>>();
    private List<List<Long>> slidingValueListOfList = new ArrayList<List<Long>>();

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
        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            previousStartTime = 0L;
            previousEndTime = ((SlidingWindow) this.window).getLength();
        }
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

                if(tumblingTimeStampListOfList.size() >=1)
                {        
                    int len = tumblingTimeStampListOfList.size();
                    if(tumblingTimeStampListOfList.get(len -1).size() != length)
                    {
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
                            
                        }else{
                            previousStartTime=startTime;
                            previousEndTime=endTime;
                            List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                            tumblingTimeStampListOfList.add(a);
                            
                            List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                            tumblingValueListOfList.add(b);
                        }
                        
                        
                    }
                    else
                    {
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
                    // add the tumbling 
                    List<Long> a = new ArrayList<Long>(tumblingListTimeStamp);               // clone of tumblingListTimeStamp.
                    tumblingTimeStampListOfList.add(a);
                    
                    // add the tumbling 
                    List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                    tumblingValueListOfList.add(b);
                }
            }
            else if(tumblingListTimeStamp.size() >= length )
            {
                tumblingListTimeStamp.clear();
                tumblingListTimeStamp.add(timeStamp);

                tumblingListValue.clear();
                tumblingListValue.add(value);
            }
            
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
        
        }

        // For sliding window
        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            length = ((SlidingWindow) this.window).getLength();
            slide = ((SlidingWindow) this.window).getSlide();
            timeStamp = event.getTimestamp();
            value     = event.getValue();
           
            slidingTimeStamp.add(timeStamp);
            slidingValue.add(value);
            /*
            if(slidingListTimeStamp.size() < length )
            {
                for(int i=0; i <= (length*2) ; i+=length)
                {
                    startTime = i;
                    endTime = i + length;
                    
                    if(timeStamp >= startTime && timeStamp < endTime)
                    {
                        if(startTime==previousStartTime && endTime==previousEndTime)
                        {
                            slidingListTimeStamp.add(timeStamp);
                            //tumblingListValue.add(value);     
                            break;
                        }
                        else
                        {
                            slidingListTimeStamp.clear();
                            //tumblingListValue.clear();
                            slidingListTimeStamp.add(timeStamp);
                            //tumblingListValue.add(value);
                            break;
                        }
                    }
                }

                if(slidingListTimeStampListOfList.size() >=1)
                {        
                    int len = slidingListTimeStampListOfList.size();
                    if(slidingListTimeStampListOfList.get(len -1).size() != length)
                    {
                        // replace the last element for timestamp
                        if(previousStartTime == startTime && previousEndTime == endTime)
                        {
                            slidingListTimeStampListOfList.remove(slidingListTimeStampListOfList.get(len -1 ));
                            List<Long> a = new ArrayList<Long>(slidingListTimeStamp);               // clone of tumblingListTimeStamp.
                            slidingListTimeStampListOfList.add(a); 

                            // replace the last element for value
                            //tumblingValueListOfList.remove(tumblingValueListOfList.get(len -1 ));
                            //List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                            //tumblingValueListOfList.add(b); 
                            
                        }else{
                            previousStartTime=startTime;
                            previousEndTime=endTime;
                            List<Long> a = new ArrayList<Long>(slidingListTimeStamp);               // clone of tumblingListTimeStamp.
                            slidingListTimeStampListOfList.add(a);
                            
                            //List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                            //tumblingValueListOfList.add(b);
                        }
                        
                        
                    }
                    else
                    {
                        // add the tumbling 
                        List<Long> a = new ArrayList<Long>(slidingListTimeStamp);               // clone of tumblingListTimeStamp.
                        slidingListTimeStampListOfList.add(a); 

                        // add the tumbling 
                        //List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                        //slidingListTimeStampListOfList.add(b); 
                    }
                }
                else
                {
                    // add the tumbling 
                    List<Long> a = new ArrayList<Long>(slidingListTimeStamp);               // clone of tumblingListTimeStamp.
                    slidingListTimeStampListOfList.add(a);
                    
                    // add the tumbling 
                    //List<Long> b = new ArrayList<Long>(tumblingListValue);               // clone of tumblingListTimeStamp.
                    //tumblingValueListOfList.add(b);
                }
            }
            else if(slidingListTimeStamp.size() >= length )
            {
                slidingListTimeStamp.clear();
                slidingListTimeStamp.add(timeStamp);

                //umblingListValue.clear();
                //tumblingListValue.add(value);
            }

            List<List<Long>> timeStampList = new ArrayList<List<Long>>();
            //List<List<Long>> valueList = new ArrayList<List<Long>>();
            for(int i=0; i<= 2; i++)
            {
                List<Long> smallerList = new ArrayList<Long>();
                //List<Long> smallerListValue = new ArrayList<Long>();
                startTime = i*length ;   
                endTime = (i+1)*length;  
                for(int j=0; j<slidingListTimeStampListOfList.size() ; j++)
                {
                    
                    for(int k=0; k<slidingListTimeStampListOfList.get(j).size(); k++)
                    {
                        if(slidingListTimeStampListOfList.get(j).get(k) >= startTime && slidingListTimeStampListOfList.get(j).get(k)<endTime)
                        {
                            smallerList.add(slidingListTimeStampListOfList.get(j).get(k));
                            //smallerListValue.add(tumblingValueListOfList.get(j).get(k));
                        }
                    }
                }
                List<Long> tempList = new ArrayList<Long>(smallerList); 
                Collections.sort(tempList);
                timeStampList.add(tempList);
                timeStampList.removeAll(Collections.singleton(new ArrayList<>()));

                //List<Long> tempListValue = new ArrayList<Long>(smallerListValue); 
                //Collections.sort(tempListValue);
                //valueList.add(tempListValue);
                //valueList.removeAll(Collections.singleton(new ArrayList<>()));
                //System.out.println("inner: " + timeStampList) ;
            }  

            System.out.println(timeStampList);
            System.out.println(slidingListTimeStampListOfList);
            */
            
            /*if( slidingEventHash.containsKey(timeStamp))
            {
                slidingEventHash.put(timeStamp, value);
            }
            else
            {
                slidingEventHash.putIfAbsent(timeStamp, value);		
            }
            System.out.println(slidingEventHash);
            */
        }
        
        if(this.window.getClass().getSimpleName().equals("SessionWindow"))
        {

            gap = ((SessionWindow) this.window).getGap();
            //long i=0L;
            timeStamp = event.getTimestamp();
            value     = event.getValue();
            
            sessionListTimeStamp.add(timeStamp);
            sessionListValue.add(value);
            if(sessionListTimeStamp.size() >= 2)
            {
                for(int i = 0 ; i < sessionListTimeStamp.size(); i++)
                {
                    if( sessionListTimeStamp.size() > i+1)
                    {
                        if((sessionListTimeStamp.get(i+1) - sessionListTimeStamp.get(i)) >= gap)
                        {
                            
                            // For the future loop
                            long varTimeStamp = sessionListTimeStamp.get(i+1);
                            long varValue = sessionListValue.get(i+1);
                            
                            // Remove the last element
                            sessionListTimeStamp.remove(i+1);
                            sessionListValue.remove(i+1);

                            // add these lists to another list.
                            List<Long> a = new ArrayList<Long>(sessionListTimeStamp);               // clone of sessionListTimeStamp.
                            List<Long> b = new ArrayList<Long>(sessionListValue);                   // clone of sessionListTimeStamp.
                            System.out.println("a: " + a);
                            Collections.sort(a);
                            Collections.sort(b);

                            sessionEventTimeStampListOfList.add(a);
                            sessionEventValueListOfList.add(b);
                  

                            sessionListTimeStamp.clear();
                            sessionListValue.clear();

                            sessionListTimeStamp.add(varTimeStamp);
                            sessionListValue.add(varValue);
                            break;
                        }
                    }
                }
            }
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

        // YOUR CODE HERE
        // Iterate over the hasmap, and then check if the given timestamp is under the watermark stamp
        final List<ResultWindow> resultList = new ArrayList<ResultWindow>();
        
        // In Order stream Test for Tumbling Window. Done. 
        if(this.window.getClass().getSimpleName().equals("TumblingWindow"))
        {
              
            for(int i = 0; i < finalTimeStamp.size(); i++)          // Over each loop, we have 1 tumbling count window
            { 
                startTime = i*length ;    
                endTime = (i+1)*length;  
                List<Long> tempa = new ArrayList<Long>();
                
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
        }

        if(this.window.getClass().getSimpleName().equals("SlidingWindow"))
        {
            Collections.sort(slidingTimeStamp);
            Collections.sort(slidingValue);
            List<Long> timeStampList = new ArrayList<Long>();
            List<Long> valueList = new ArrayList<Long>();
            for(int i=0; i<=15; i+=slide)
            {
                startTime = i ;   
                endTime = i+length;  

                timeStampList.clear();
                valueList.clear();

                for(int j =0 ; j< slidingTimeStamp.size(); j++)
                {
                    if(slidingTimeStamp.get(j) >= startTime && slidingTimeStamp.get(j) < endTime)
                    {
                        if(previousStartTime == startTime && previousEndTime == endTime)
                        {
                            timeStampList.add(slidingTimeStamp.get(j));
                            valueList.add(slidingValue.get(j));
                            List<Long> a = new ArrayList<Long>(timeStampList);
                            slidingTimeStampListOfList.add(a);
                            List<Long> b = new ArrayList<Long>(valueList);
                            slidingValueListOfList.add(b);
                        }
                    }
                    else
                    {
                        slidingTimeStampListOfList.removeAll(Collections.singleton(new ArrayList<>()));
                        slidingValueListOfList.removeAll(Collections.singleton(new ArrayList<>()));
                        // clear all the elements in tumblingTimeStamp which are present in timeStampList
                        // i.e deleting elements which are processed.
                        for(int k=0; k < timeStampList.size();k++)
                        {
                            for(int l=0; l < slidingTimeStamp.size(); l++)
                            {
                                if(slidingTimeStamp.get(l) < (endTime-slide)  )
                                {
                                    slidingTimeStamp.remove(l);
                                    slidingValue.remove(l);
                                }
                            }
                        }
                        previousStartTime = startTime+slide;
                        previousEndTime = endTime+slide;
                    }
                }
                if((watermarkTimestamp - endTime <= slide) && (watermarkTimestamp >= endTime))
                {
                    if(slidingTimeStampListOfList.size() > 0)
                    {
                        sum = this.aggregateFunction.aggregate(slidingValueListOfList.get(slidingValueListOfList.size()-1));
                        ResultWindow r = new ResultWindow(startTime, endTime, sum);
                        //final List<ResultWindow> resultList = new ArrayList<ResultWindow>( Arrays.asList(r) ) ;
                        resultList.add(r);
                    }
                }
            }

            /*
            if(timeStampList.size()>0)
            {
                System.out.println("runnning");
                List<Long> a = new ArrayList<Long>(timeStampList);
                slidingTimeStampListOfList.add(a);
                List<Long> b = new ArrayList<Long>(valueList);
                slidingValueListOfList.add(b);                
            }
            */
            //System.out.println(slidingTimeStampListOfList);
            //System.out.println(slidingValueListOfList);

            //slidingTimeStampListOfList.clear();
            //slidingValueListOfList.clear();

            /*long i = 0L;
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
            }*/
        }
        
        if(this.window.getClass().getSimpleName().equals("SessionWindow"))
        {
            for(int i =0; i < sessionEventTimeStampListOfList.size(); i++)          // Over each loop, we have 1 session window
            {   
                for(int j=0; j <sessionEventTimeStampListOfList.get(i).size() ; j++)
                {
                    // Ignore any evenet coming after the watermark timestamp.
                    if(sessionEventTimeStampListOfList.get(i).get(j) > watermarkTimestamp)
                    {
                        sessionEventTimeStampListOfList.get(i).remove(j);
                        sessionEventValueListOfList.get(i).remove(j);
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
