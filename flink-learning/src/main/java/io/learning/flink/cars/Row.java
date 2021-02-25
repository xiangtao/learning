package io.learning.flink.cars;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 代表一行数据
 */
public class Row {
    public static final String SOURCE_CONFIG_KEY = "__SOURCE_CONFIG_KEY";

    private Map<String, Object> map = new HashMap<String, Object>();

    /**
     * 得到相应列的数据
     *
     * @param name
     * @return
     */
    public Object get(String name) {
        if (map != null) {
            return map.get(name);
        }
        return null;
    }

    public void set(String name, Object val) {
        map.put(name, val);
    }

    public void remove(String name) {
        this.map.remove(name);
    }

    public int size() {
        return map.size();
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void add(Row row) {
        if (row == null) {
            return;
        }
        this.map.putAll(row.getMap());
    }

    @Override
    public String toString() {
        return "Row{" +
                "map=" + map +
                '}';
    }

    /**
     * Row 多字段排序比较器
     * 默认desc 排序
     */
    public static class RowComparator implements Comparator<Row> {
        private String[] sortFiled;
        private SortType[] sortType;

        public RowComparator(String[] sortFiled) {
            this.sortFiled = sortFiled;
        }

        public RowComparator(String[] sortFiled, SortType[] sortType) {
            this.sortFiled = sortFiled;
            this.sortType = sortType;
        }

        @Override
        public int compare(Row o1, Row o2) {
            for (int i = 0; i < sortFiled.length; i++) {
                Object val1 = o1.get(sortFiled[i]);
                Object val2 = o2.get(sortFiled[i]);
                SortType sort = sortType[i];
                int cp = getDescOrder(val1, val2);
                int compareResult = SortType.DESC.equals(sort) ? cp : -1 * cp;
                if (compareResult == 0) {
                    continue;
                } else {
                    return compareResult;
                }
            }
            return 0;
        }

        private int getDescOrder(Object val1, Object val2) {
            if (val1 instanceof Timestamp) {
                Timestamp innerVal1 = (Timestamp) val1;
                Timestamp innerVal2 = (Timestamp) val2;
                return innerVal2 == null ? -1 : (innerVal2.compareTo(innerVal1));
            } else if (val1 instanceof Date) {
                Date innerVal1 = (Date) val1;
                Date innerVal2 = (Date) val2;
                return innerVal2 == null ? -1 : (innerVal2.compareTo(innerVal1));
            } else if (val1 instanceof Long) {
                Long innerVal1 = (Long) val1;
                Long innerVal2 = (Long) val2;
                return innerVal2 == null ? -1 : (innerVal2.compareTo(innerVal1));
            } else if (val1 instanceof Integer) {
                Integer innerVal1 = (Integer) val1;
                Integer innerVal2 = (Integer) val2;
                return innerVal2 == null ? -1 : (innerVal2.compareTo(innerVal1));
            } else {
                if (val1 == null) {
                    return val2 != null ? 1 : -1;
                } else if (val2 == null) {
                    return -1;
                }
                return (val2 + "").compareTo(val1 + "");
            }
        }


    }

    public static enum SortType {
        /**
         * Column will be sorted in an ascending order.
         */
        ASC,

        /**
         * Column will be sorted in a descending order.
         */
        DESC;

        // UNSORTED
    }
}

