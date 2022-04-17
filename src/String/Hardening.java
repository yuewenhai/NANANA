package String;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Hardening {
    public String type;
    public int bfLen;

    Hardening(String type, int bfLen_) {
        this.type = type;
        this.bfLen = bfLen_;
    }

    public BitSet harden_bf(BitSet bf) {
        return null;
    }
    public void set_non_secret_index_list(String bitFreqFilePath) {}
    public void set_indexGroup_list(String bitFreqFilePath){}
}

class Balancing extends Hardening {
    public int random_seed = 42;
    public List<Integer> shuffle_indexes = null;

    Balancing(String type, int bf_len_) {
        super(type, bf_len_);
        init_shuffle_index_list(this.bfLen);
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        BitSet balanced_bf = new BitSet(this.bfLen * 2);
        for (int i = 0; i < this.bfLen; i++) {
            if (bf.get(i)) balanced_bf.set(i);
            else balanced_bf.set(i + this.bfLen);
        }

        BitSet shuffle_balanced_bf = new BitSet(this.bfLen * 2);

        for (int i = 0; i < this.bfLen * 2; i++) {
            int index = this.shuffle_indexes.get(i);
            if (balanced_bf.get(index)) shuffle_balanced_bf.set(i);
        }
        return shuffle_balanced_bf;
    }

    private void init_shuffle_index_list(int bf_len) {
        if (this.shuffle_indexes == null) {
            System.out.println("Initialize the shuffle index list");
            Random random = new Random();
            random.setSeed(this.random_seed);

            List<Integer> index_list = new ArrayList<>();
            for (int i = 0; i < bf_len * 2; i++) {
                index_list.add(i);
            }

            Collections.shuffle(index_list, random);
            this.shuffle_indexes = index_list;
        }
    }
}

class XorFolding extends Hardening {
    XorFolding(String type, int bf_len_) {
        super(type, bf_len_);
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        int bf_len = this.bfLen / 2;
        BitSet pre_half_bf = bf.get(0, bf_len - 1);
        BitSet suf_half_bf = bf.get(bf_len, this.bfLen - 1);

        pre_half_bf.xor(suf_half_bf);
        return pre_half_bf;
    }
}


class Rule90 extends Hardening {

    Rule90(String type, int bfLen_) {
        super(type, bfLen_);
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        BitSet rule90_bf = new BitSet(this.bfLen);
        for (int i = 0; i < this.bfLen; i++) {
            if (i == 0) {
                if (bf.get(this.bfLen - 1) != bf.get(i + 1)) rule90_bf.set(i);
            } else if (i == this.bfLen - 1) {
                if (bf.get(i - 1) != bf.get(0)) rule90_bf.set(i);
            } else {
                if (bf.get(i - 1) != bf.get(i + 1)) rule90_bf.set(i);
            }
        }

        return rule90_bf;
    }
}

class Blip extends Hardening {
    public double prob;

    Blip(String type, double prob, int bfLen_) {
        super(type, bfLen_);
        this.prob = prob;
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        Random random = new Random();
        for (int i = 0; i < this.bfLen; i++) {
            double coin1 = random.nextDouble();
            if (coin1 <= this.prob) {
                double coin2 = random.nextDouble();
                if (coin2 <= 0.5) bf.set(i);
                else bf.clear(i);
            }
        }

        return bf;
    }
}


class Wxor extends Hardening {
    public int w_size;

    Wxor(String type, int w_size, int bfLen_) {
        super(type, bfLen_);
        this.type = String.format("WXOR %d", w_size);
        this.w_size = w_size;
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        for (int i = 0; i < this.bfLen - this.w_size + 1; i++) {
            BitSet w1 = bf.get(i, i + this.w_size - 1);
            BitSet w2 = bf.get(i + 1, i + this.w_size);
            w1.and(w2);
            for (int j = 0;j < this.w_size;j++){
                if (w1.get(j)) bf.set(i + j);
            }
        }
        return bf;
    }
}

class ResamXor extends Hardening {
    public int random_seed = 42;
    public List<int[]> index_pairs = null;

    ResamXor(String type, int bf_len) {
        super(type, bf_len);
        init_index_pair_list(bf_len);
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        BitSet resam_bf = new BitSet(this.bfLen);

        for (int i = 0; i < this.bfLen; i++) {
            int[] index_pair = this.index_pairs.get(i);
            if (bf.get(index_pair[0]) != bf.get(index_pair[1])) resam_bf.set(i);
        }
        return resam_bf;
    }

    private void init_index_pair_list(int bf_len) {
        if (this.index_pairs == null) {
            this.index_pairs = new ArrayList<>();
            Random random = new Random();
            random.setSeed(this.random_seed);
            for (int i = 0; i < bf_len; i++) {
                int random_value1 = random.nextInt(bf_len);
                int random_value2 = random.nextInt(bf_len);
                int[] index_pair = {random_value1, random_value2};
                index_pairs.add(index_pair);
            }
        }
    }
}


class Urap extends Hardening {
    public double non_secret_ratio;
    public double epsilon;
    public double theta;
    public List<Integer> non_secret_index_list = null;
    public double d1;
    public double d2;
    public String dataset_file;
    public String encode_type;
    public String hash_type;
    public int q;
    public boolean padded;
    public String num_hash_function;

    Urap(String type, double non_secret_ratio, double epsilon, String dataset_file, String encode_type, String hash_type, int q,
         boolean padded, String num_hash_function, int bf_len) {
        super(type, bf_len);
        this.non_secret_ratio = non_secret_ratio;
        this.epsilon = epsilon;
        this.theta = Math.exp(epsilon / 2) / (Math.exp(epsilon / 2) + 1);
        this.d1 = this.theta / (this.theta + (1 - this.theta) * Math.exp(this.epsilon));
        this.d2 = (this.theta + (1 - this.theta) * Math.exp(this.epsilon)) / Math.exp(this.epsilon);
        this.dataset_file = dataset_file;
        this.encode_type = encode_type;
        this.hash_type = hash_type;
        this.q = q;
        this.padded = padded;
        this.num_hash_function = num_hash_function;
    }

    @Override
    public void set_non_secret_index_list(String bitFreqFilePath) {
        String freq_file = bitFreqFilePath + String.format("%s-%s-%d-%s-%s.csv",
                encode_type.toLowerCase(Locale.ROOT), hash_type, q,
                String.valueOf(padded).toLowerCase(Locale.ROOT), num_hash_function);
        try (FileReader fr = new FileReader(freq_file);
             BufferedReader br = new BufferedReader(fr)) {
            String line = br.readLine();
            List<Integer> order_index_freq_list = new ArrayList<>();
            List<Integer> order_index_list = new ArrayList<>();
            int freq = 0;
            int index = 0;
            while ((line = br.readLine()) != null) {
                freq = Integer.parseInt(line);
                if (index == 0) {
                    order_index_freq_list.add(freq);
                    order_index_list.add(index);
                    index += 1;
                    continue;
                }
                // sort frequency along with index
                for (int i = 0; i < order_index_freq_list.size(); i++) {
                    if (order_index_freq_list.get(i) >= freq) {
                        order_index_freq_list.add(i, freq);
                        order_index_list.add(i, index);
                        break;
                    } else if (i == order_index_freq_list.size() - 1) {
                        order_index_freq_list.add(freq);
                        order_index_list.add(index);
                        break;
                    }
                }
                index += 1;
            }

            int non_secret_len = (int) Math.round(non_secret_ratio * this.bfLen);
            non_secret_index_list = order_index_list.subList(0, non_secret_len);
            Collections.sort(non_secret_index_list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BitSet harden_bf(BitSet bf) {

        Random random = new Random();

        BitSet urap_bf = new BitSet(this.bfLen);
        assert this.non_secret_index_list != null;
        for (int i = 0; i < this.bfLen; i++) {
            double rand_value = random.nextDouble();
            if (this.non_secret_index_list.contains(i)) {
                // 如果不在敏感集里
                // 0 必是 0
                // 1 1 - d2是1;d2是0
                if (bf.get(i)) {
                    if (rand_value <= 1 - this.d2) urap_bf.set(i);
                }
            } else {
                // 如果在敏感集里
                // 0 d1是1;1-d1是0
                // 1 theta是1;1-theta是0
                if (bf.get(i)) {
                    if (rand_value <= this.theta) urap_bf.set(i);
                } else {
                    if (rand_value <= this.d1) urap_bf.set(i);
                }
            }
        }

        return urap_bf;
    }
}

class UrapProb extends Urap {
    public List<Integer> non_secret_index_list = null;
    public double flip_prob_secret;
    public double flip_prob_non_secret;

    UrapProb(String type, double non_secret_ratio, double prob_secret, double prob_non_secret, String dataset_file,
             String encode_type, String hash_type, int q, boolean padded, String num_hash_function, int bf_len) {
        super(type, non_secret_ratio, 0.0, dataset_file, encode_type, hash_type, q, padded, num_hash_function, bf_len);
        this.flip_prob_secret = prob_secret;
        this.flip_prob_non_secret = prob_non_secret;
    }

    @Override
    public BitSet harden_bf(BitSet bf) {

        Random random = new Random();

        BitSet urap_bf = new BitSet(this.bfLen);
        assert this.non_secret_index_list != null;
        double coin1;
        double coin2;
        for (int i = 0; i < this.bfLen; i++) {
            if (this.non_secret_index_list.contains(i)) {
                // 如果不在敏感集里
                // 0 必是 0
                // 1 1 - d2是1;d2是0
                if (bf.get(i)) {
                    coin1 = random.nextDouble();
                    if (coin1 <= flip_prob_non_secret) {
                        coin2 = random.nextDouble();
                        if (coin2 <= 0.5) bf.set(i);
                        else bf.clear(i);
                    }
                }
            } else {
                // 如果在敏感集里
                // blip
                coin1 = random.nextDouble();
                if (coin1 <= flip_prob_secret) {
                    coin2 = random.nextDouble();
                    if (coin2 <= 0.5) bf.set(i);
                    else bf.clear(i);
                }
            }
        }

        return urap_bf;
    }
}

class IndexD extends Urap {
    public List<String> epsilons;
    public List<List<Integer>> indexGroup;

    IndexD(String type, List<String> epsilons_, String dataset_file, String encode_type, String hash_type, int q,
           boolean padded, String num_hash_function, int bf_len) {
        super(type, 0, 0.0, dataset_file, encode_type, hash_type, q, padded, num_hash_function, bf_len);
        epsilons = epsilons_;
        indexGroup = new ArrayList<>();
    }

    @Override
    public void set_indexGroup_list(String bitFreqFilePath) {
        String freq_file = bitFreqFilePath + String.format("%s-%s-%d-%s-%s.csv",
                encode_type.toLowerCase(Locale.ROOT), hash_type, q,
                String.valueOf(padded).toLowerCase(Locale.ROOT), num_hash_function);
        try (FileReader fr = new FileReader(freq_file);
             BufferedReader br = new BufferedReader(fr)) {
            // initial the priority queue
            int size = this.epsilons.size();
            List<PriorityQueue<int[]>> priorityQueueList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                PriorityQueue<int[]> priorityQueue = new PriorityQueue<>(new Comparator<int[]>() {
                    @Override
                    public int compare(int[] o1, int[] o2) {
                        return o2[0] - o1[0];
                    }
                });
                priorityQueueList.add(priorityQueue);
            }
            int queueSize = this.bfLen / size + 1;
            // read the file
            String line = br.readLine();
            int freq = 0;
            int index = 0;
            while ((line = br.readLine()) != null) {
                freq = Integer.parseInt(line);
                PriorityQueue<int[]> tempQueue;
                int[] add_item = new int[]{freq, index};
                for (int i = 0; i < size; i++) {
                    tempQueue = priorityQueueList.get(i);
                    tempQueue.add(add_item);
                    if (tempQueue.size() >= queueSize) {
                        add_item = tempQueue.poll();
                    } else break;
                }
                index += 1;
            }
            // store and sort the index
            for (PriorityQueue<int[]> priorityQueue : priorityQueueList) {
                List<Integer> indexList = new ArrayList<>();
                while (!priorityQueue.isEmpty()) indexList.add(priorityQueue.poll()[1]);
                Collections.sort(indexList);
                indexGroup.add(indexList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BitSet harden_bf(BitSet bf) {
        double coin1;
        double coin2;
        Random random = new Random();
        for (int i = 0; i < this.bfLen; i++) {
            // 暂时epsilon是概率
            double epsilon = 0.0;
            // find the group which contains the index
            for (int j = 0; j < epsilons.size(); j++) {
                if (indexGroup.get(j).contains(i)) {
                    epsilon = Double.parseDouble(epsilons.get(j));
                    break;
                }
            }
//            double prob = Math.exp(epsilon) / (Math.exp(epsilon) + 1);
            double prob = epsilon;
            coin1 = random.nextDouble();
            if (coin1 <= prob) {
                coin2 = random.nextDouble();
                if (coin2 <= 0.5) bf.set(i);
                else bf.clear(i);
            }
        }

        return bf;
    }
}

