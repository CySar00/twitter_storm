package storm.bolt.FusionLists.Functions;

import java.text.ParseException;
import java.util.*;

/**
 * Created by christina on 5/4/15.
 */
public class RankAggregation<T> {

    @SuppressWarnings({"unchecked"})
    public List<T> sort(final Map<T, Double> scores, final boolean reverse) {
        Comparator comparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                T key1 = (T) o1;
                T key2 = (T) o2;
                Comparable comparable1 = (Comparable) scores.get(key1);
                Comparable comparable2 = (Comparable) scores.get(key2);

                int c = reverse ? -comparable1.compareTo(comparable2) : comparable1.compareTo(comparable2);
                if (0 != c) {
                    return c;
                }
                Integer h1 = key1.hashCode();
                Integer h2 = key2.hashCode();

                return reverse ? -h1.compareTo(h2) : h1.compareTo(h2);
            }
        };
        List<T> result = new ArrayList<T>();
        result.addAll(scores.keySet());
        Collections.sort(result, comparator);
        return result;
    }

    public Map<T, Double> normalize(Map<T, Double> scores) {
        Map<T, Double> result = new HashMap<T, Double>();
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        for (Double score : scores.values()) {

            if (score < min) {
                min = score;
            }

            if (score > max) {
                max = score;
            }
        }

        for (T object : scores.keySet()) {
            double score = scores.get(object);
            if ((max - min) == 0.0) {
                result.put(object, 0.0);
            } else {
                result.put(object, (score - min) / (max - min));
            }
        }
        return result;
    }

    @SuppressWarnings({"unchecked"})
    public Map<T, Double>[] processMap(Map<T, Map<String, Double>> maps) {
        Map<String, Integer> metricToID = new HashMap<String, Integer>();
        for (Map<String, Double> aux : maps.values()) {
            for (String metric : aux.keySet()) {
                Integer id = metricToID.get(metric);
                if (id == null) {
                    metricToID.put(metric, metricToID.size());
                }
            }
        }
        HashMap<T, Double> newMaps[] = (HashMap<T, Double>[]) (new HashMap[metricToID.size()]);
        for (int i = 0; i < newMaps.length; i++) {
            newMaps[i] = new HashMap<T, Double>();
        }

        for (T object : maps.keySet()) {
            Map<String, Double> aux = maps.get(object);
            for (String metric : aux.keySet()) {
                newMaps[metricToID.get(metric)].put(object, aux.get(metric));
            }
        }
        return newMaps;
    }

    public Map<T, Double> reciprocalRankFusion(Map<T, Map<String, Double>> maps) {
        return reciprocalRankFusion(processMap(maps));
    }

    public Map<T, Double> bordaFusion(Map<T, Map<String, Double>> maps) {
        return bordaFusion(processMap(maps));
    }

    public Map<T, Double> combSUM(Map<T, Map<String, Double>> maps) {
        return combSUM(processMap(maps));
    }

    public Map<T, Double> combMNZ(Map<T, Map<String, Double>> maps) {
        return combMNZ(processMap(maps));
    }

    @SuppressWarnings({"unchecked"})
    public Map<T, Double> reciprocalRankFusion(Map<T, Double> maps[]) {
        Map<T, Double> result = new HashMap<T, Double>();
        List<T> sorted[] = (List<T>[]) (new ArrayList[maps.length]);
        for (int i = 0; i < maps.length; i++) {
            sorted[i] = sort(maps[i], true);
        }

        for (T object : maps[0].keySet()) {
            double score = 0;
            for (List<T> list : sorted) {
                for (int i = 0; i < list.size(); i++) {

                    if (list.get(i).equals(object)) {
                        score += 1.0 / (i + 60.0);
                        break;
                    }
                }
            }
            result.put(object, score);
        }
        return result;
    }

    @SuppressWarnings({"unchecked"})
    public Map<T,Double>bordaFusion(Map<T,Double>maps[]){
        Map<T,Double>result=new HashMap<T, Double>();
        List<T>sorted[]=(List<T>[])(new ArrayList[maps.length]);

        for(int i=0;i<maps.length;i++){
            sorted[i]=sort(maps[i],true);
        }

        for(T obj:maps[0].keySet()){
            double score=0;
            for(List<T>list:sorted){
                for(int i=0;i<list.size();i++){
                    if(list.get(i).equals(obj)){
                        score+=(list.size()-i-1);
                        break;
                    }
                }
            }
            result.put(obj,score);
        }
        return result;
    }

    public Map<T,Double>combSUM(Map<T,Double>maps[]){
        Map<T,Double>result=new HashMap<T, Double>();
        for(int i=0;i<maps.length;i++){
            maps[i]=normalize(maps[i]);
        }

        for(T object:maps[0].keySet()){
            double score=0;
            for(int i=0;i<maps.length;i++){
                score+=maps[i].get(object);
            }
        }
        return result;
    }
    public Map<T,Double>combMNZ(Map<T,Double>maps[]){
        Map<T,Double>result=new HashMap<T, Double>();
        for(int i=0;i<maps.length;i++){
            maps[i]=normalize(maps[i]);
        }

        for(T obj:maps[0].keySet()){
            double score=0;
            double nonZero=0;

            for(int i=0;i<maps.length;i++){
                score+=maps[i].get(obj);
                nonZero+=maps[i].get(obj) !=0.0 ? 1.0: 0.0;
            }
            result.put(obj,score*nonZero);
        }
        return result;
    }
}
