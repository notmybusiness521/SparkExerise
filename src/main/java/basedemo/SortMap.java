package basedemo;

import java.util.*;

/**
 * 功能描述: <br>
 *
 * @since: 1.0.0
 * @Author:wjp
 * @Date: 2020/4/11 22:23
 */

public class SortMap {
    public static void main(String[] args) {
        HashMap<String, Double> map = new HashMap<String, Double>();
        ValueComparator bvc = new ValueComparator(map);
        TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);

        map.put("E", 49.5);
        map.put("C", 67.4);
        map.put("B", 67.4);
        map.put("D", 67.3);

        System.out.println("unsorted map: " + map);
        sorted_map.putAll(map);
        System.out.println("results: " + sorted_map);
        Map<String, Double> res = sortMap(map);
        System.out.println(res.toString());
    }

    public static <K extends Comparable<? super K>, V extends Comparable<? super V>> Map<K, V> sortMap(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        //下面是按Key或者Value排序，正序或者倒序只需要保留对应的那行代码，注释其余三行即可

        //按Key排序
        list.sort(Map.Entry.comparingByKey());//按key排序正序
        list.sort(Map.Entry.comparingByKey(Comparator.reverseOrder()));//按key排序倒序
        //按Value排序
        list.sort(Map.Entry.comparingByValue());//按value排序正序
        list.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));//按value排序倒序

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

}
class ValueComparator implements Comparator<String> {
    Map<String, Double> base;

    public ValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}