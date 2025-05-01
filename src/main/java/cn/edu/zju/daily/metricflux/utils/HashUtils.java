package cn.edu.zju.daily.metricflux.utils;

import smile.hash.MurmurHash3;

public class HashUtils {

    public static <K> int hash(K element) {
        int hashCode = element.hashCode();
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (hashCode & 0xFF);
        bytes[1] = (byte) ((hashCode >> 8) & 0xFF);
        bytes[2] = (byte) ((hashCode >> 16) & 0xFF);
        bytes[3] = (byte) ((hashCode >> 24) & 0xFF);
        return MurmurHash3.hash32(bytes, 0, 4, 38324);
    }

    public static int hashString(String element) {
        return MurmurHash3.hash32(element, 0);
    }

    public static <K> int hashPartition(K key, int numWorkers) {
        //        int hash;
        //        if (key instanceof Integer) {
        //            hash = Integer.hashCode((Integer) key);
        //        } else {
        //            hash = HashUtils.hashString(key.toString());
        //        }
        //
        //        int mod = Math.floorMod(hash, 2 * numWorkers);
        //        if (mod < numWorkers) {
        //            return mod;
        //        } else {
        //            return 2 * numWorkers - 1 - mod;
        //        }
        int hash = HashUtils.hashString(key.toString()); // align with Python mh3 hash
        return Math.floorMod(hash, numWorkers);
    }

    public static void main(String[] args) {
        System.out.println(HashUtils.hashString("hello"));
        System.out.println(Math.floorMod(-5, 3));
    }
}
