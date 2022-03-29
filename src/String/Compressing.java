package String;

import java.util.*;

public class Compressing {
    private final int randomSeed = 42;
    public double ratio;

    public Compressing(int ratio) {
        this.ratio = ratio;
    }

    public BitSet compressing(BitSet bf, int bf_len) {
        double compressDouble = bf_len * ratio;
        int compressCnt = (int) compressDouble;

        Random rnd = new Random();
        rnd.setSeed(randomSeed);
        int random_index1;
        int random_index2;
        List<Integer> remove_index_list = new ArrayList<>();
        // find the index to be removed
        // if index1 == index2, keep the first
        // if index1 != index2, keep the set index
        for (int i = 0; i < compressCnt; i++) {
            random_index1 = rnd.nextInt(bf_len);
            random_index2 = rnd.nextInt(bf_len);

            if (bf.get(random_index1) == bf.get(random_index2))
                remove_index_list.add(Math.max(random_index1, random_index2));
            else if (bf.get(random_index1)) remove_index_list.add(random_index2);
            else remove_index_list.add(random_index1);
        }
        Collections.sort(remove_index_list);

        int compressed_bf_len = bf_len - compressCnt;
        BitSet compressed_bf = new BitSet(compressed_bf_len);
        for (int i = 0, j = 0; i < bf_len; i++) {
            assert j < remove_index_list.size();
            if (i == remove_index_list.get(j)) {
                j++;
                continue;
            }
            if (bf.get(i)) compressed_bf.set(i - j);
        }

        return compressed_bf;
    }
}
