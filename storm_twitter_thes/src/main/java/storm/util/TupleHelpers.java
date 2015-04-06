package storm.util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * Created by christina on 3/26/15.
 */
public final class TupleHelpers  {

    private TupleHelpers(){}

    public static boolean isTickTuple(Tuple tuple){
        return  tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceComponent().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}


