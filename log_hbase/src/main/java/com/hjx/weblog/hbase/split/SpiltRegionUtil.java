package com.hjx.weblog.hbase.split;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author:
 * @description: hbase分区算法工具类
 * @Date:Created in 2019-04-03 09:49
 */
public class SpiltRegionUtil {

    /**
     * 定义分区
     * @return
     */
    public static byte[][] getSplitKeysBydinct() {

        String[] keys = new String[]{"1","2", "3","4", "5","6", "7","8", "9","a","b", "c","d","e","f"};
        //String[] keys = new String[]{"10|", "20|", "30|", "40|", "50|", "60|", "70|", "80|", "90|"};
        byte[][] splitKeys = new byte[keys.length][];

        //通过treeset排序
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }


    public static void main(String[] args) {
        byte[][] split = getSplitKeysBydinct();
        for (byte[] bytes : split) {

            for (byte aByte : bytes) {
                System.out.println(aByte);
            }
            System.out.println("---------------------");
        }
    }



    /**
     * 定义分区
     * @return
     */
    public static byte[][] getSplitKeysByNumber() {

        String[] keys = new String[]{"10|", "11|", "12|", "13|", "14|", "15|", "16|", "17|", "18|", "19|"};
        byte[][] splitKeys = new byte[keys.length][];
        //通过treeset排序
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

}
