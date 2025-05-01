package cn.edu.zju.daily.metricflux.partitioner.containerv2;

import cn.edu.zju.daily.metricflux.utils.HashUtils;
import edu.princeton.cs.randomhash.RandomHashFamily;

class HashFamily<K> extends RandomHashFamily {

    HashFamily(int maxNumHashes) {
        super(38324, maxNumHashes);
    }

    public int baseHash(K key) {
        return HashUtils.hash(key);
    }

    public void hashes(K key, int numHashes, long[] hashes) {
        long baseHash = baseHash(key);
        for (int i = 0; i < numHashes; i++) {
            hashes[i] =
                    RandomHashFamily.truncateLong(
                            RandomHashFamily.affineTransform(
                                    baseHash, numsCoprime[i], numsNoise[i]));
        }
    }
}
