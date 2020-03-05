package videoInfoJobTop10;

import java.util.*;

public class MapUtils {
    /**
     * a sorting method, sort value in descending order
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        //sort in descending order
        Collections.sort(list, (o1, o2) -> {
            int compare = (o1.getValue()).compareTo(o2.getValue());
            return -compare;
        });
        Map<K, V> returnMap = new LinkedHashMap<>();
        for(Map.Entry<K, V> entry : list){
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }
}
