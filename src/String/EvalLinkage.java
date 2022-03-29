package String;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.CRC32;

public class EvalLinkage {

    private final int DECIMAL_PLACES = 2; // Number of decimal places to use in rounding
    private final int MAX_BLOCK_SIZE = 100; // Remove all blocks larger than this to limit memory use
    private final double PLOT_RATIO = 0.5;
    public Encoding encoding_method;
    public Hashing hashing_method;
    public Hardening hardening_method;

    public EvalLinkage(Encoding encoding, Hashing hashing, Hardening hardening) {
        this.encoding_method = encoding;
        this.hashing_method = hashing;
        this.hardening_method = hardening;
    }

    public List<Map> load_dataset_salt(String dataset_file, int[] attr_index_list, int entity_id_col,
                                       int salt_attr_col) {
        // read dataset in condition with attr_index_list
        System.out.printf("load dataset from %s\n", dataset_file);
        List<Map> result = new ArrayList<>();
        Map<String, List<String>> rec_attr_val_dict = new HashMap<>();
        Map<String, String> salt_dict = new HashMap<>();
        File file = new File(dataset_file);
        if (!file.exists()) {
            System.out.printf("dataset: %s does not exist!\n", dataset_file);
        } else {
            try (FileReader fr = new FileReader(file);
                 BufferedReader br = new BufferedReader(fr)) {
                String[] datasetColumns = br.readLine().replace("\"", "").split(",");
                List<String> attr_name = new ArrayList<>();
                for (int index : attr_index_list) {
                    attr_name.add(datasetColumns[index]);
                }

                String line = br.readLine();
                String[] lineArray;
                String entity_id;
                String salt;
                while (line != null) {
                    List<String> rec_attr_val_list = new ArrayList<>();
                    lineArray = line.replace("\"", "").split(",");
                    for (int index : attr_index_list) {
                        rec_attr_val_list.add(lineArray[index]);
                    }
                    entity_id = lineArray[entity_id_col];
                    rec_attr_val_dict.put(entity_id, rec_attr_val_list);
                    if (salt_attr_col != -1) {
                        salt = lineArray[salt_attr_col];
                        salt_dict.put(entity_id, salt);
                    }
                    line = br.readLine();
                }
                System.out.printf("load attributes: %s%n", String.join(",", attr_name));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        result.add(rec_attr_val_dict);
        if (salt_attr_col == -1)
            salt_dict = null;
        result.add(salt_dict);
        return result;
    }

    public Map<String, BitSet> gen_clk_bf_dict(Map<String, List<String>> rec_q_gram_dict, Map<String, String> salt_dict,
                                               String hardening_type, int bf_len) {
        Map<String, BitSet> rec_clk_bf_dict = new HashMap<>(); // The dictionary of Bloom filters.


        //Multi-thread generate bloom filter
        List<Thread> threadList = new ArrayList<>();
        int total_entity = rec_q_gram_dict.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = total_entity / threadNum + 1;
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, total_entity);
            Thread thread = new Thread(() -> {
                List<String> use_rec_q_gran_list;
                List<String> rec_q_gram_dict_keyList = rec_q_gram_dict.keySet().stream().toList();
                for (int j = start; j < end; j++) {
                    String entity_id = rec_q_gram_dict_keyList.get(j);
                    use_rec_q_gran_list = rec_q_gram_dict.get(entity_id);
                    // If required apply salting or Markov chain hardening
                    BitSet rec_bf;
                    //TODO MC
//            if (hardening_type.startsWith("mc")){
//                rec_bf = encoding_method.encode(use_rec_q_gran_list, true);
//            if (hardening_method != null && hardening_type.startsWith("urap")) {
                    //TODO 统计Xn比例
//            }
                    if (hardening_method != null) {
                        rec_bf = encoding_method.clk_encode(use_rec_q_gran_list);
                        rec_bf = hardening_method.harden_bf(rec_bf, bf_len);
                    } else if (hardening_type.equals("salt")) {
                        rec_bf = encoding_method.clk_encode(use_rec_q_gran_list, salt_dict.get(entity_id));
                    } else {
                        // No hardening
                        rec_bf = encoding_method.clk_encode(use_rec_q_gran_list);
                    }
//            if hardening_method is not None and hardening_method.type.startswith("Urap"):
//            print("%.2f records in Xn" % (num_in_Xn / len(rec_attr_val_dict)))

                    // Add the generated Bloom filters into the dictionary
                    //
                    synchronized (rec_clk_bf_dict) {
                        rec_clk_bf_dict.put(entity_id, rec_bf);
                    }
                }
            });
            thread.start();
            threadList.add(thread);
        }

        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        String harden_method_str;
        if (hardening_method == null) harden_method_str = hardening_type;
        else harden_method_str = hardening_method.type;
        System.out.printf("Generated %d Bloom filters by %s & %s-%d & %s%n", rec_clk_bf_dict.size(), encoding_method.type,
                hashing_method.type, hashing_method.num_hash_function, harden_method_str);
        return rec_clk_bf_dict;
    }

    public Map<String, BitSet> gen_rbf_bf_dict(Map<String, List<String>> rec_attr_dict, Map<String, String> salt_dict,
                                               String hardening_type, int bf_len) {
        Map<String, BitSet> rec_rbf_bf_dict = new HashMap<>(); // The dictionary of Bloom filters.

        //Multi-thread generate bloom filter
        List<Thread> threadList = new ArrayList<>();
        int total_entity = rec_attr_dict.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() - 1;
        int num_entity_per_thread = total_entity / threadNum + 1;
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, total_entity);
            Thread thread = new Thread(() -> {
                List<String> rec_attr_dict_keyList = rec_attr_dict.keySet().stream().toList();
                List<String> use_rec_attr_val_list;
                String entity_id;
                for (int j = start; j < end; j++) {
                    entity_id = rec_attr_dict_keyList.get(j);
                    use_rec_attr_val_list = rec_attr_dict.get(entity_id);
                    // If required apply salting or Markov chain hardening
                    BitSet rec_bf;
                    //TODO MC
//            if (hardening_type.startsWith("mc")){
//                rec_bf = encoding_method.encode(use_rec_q_gran_list, true);
//            if (hardening_method != null && hardening_type.startsWith("urap")) {
                    //TODO 统计Xn比例
//            }
                    if (hardening_method != null) {
                        rec_bf = encoding_method.rbf_encode(use_rec_attr_val_list, bf_len);
                        rec_bf = hardening_method.harden_bf(rec_bf, bf_len);
                    } else if (hardening_type.equals("salt")) {
                        rec_bf = encoding_method.rbf_encode(use_rec_attr_val_list, salt_dict.get(entity_id), bf_len);
                    } else {
                        // No hardening
                        rec_bf = encoding_method.rbf_encode(use_rec_attr_val_list, bf_len);
                    }
//            if hardening_method is not None and hardening_method.type.startswith("Urap"):
//            print("%.2f records in Xn" % (num_in_Xn / len(rec_attr_val_dict)))

                    // Add the generated Bloom filters into the dictionary
                    //
                    synchronized (rec_rbf_bf_dict) {
                        rec_rbf_bf_dict.put(entity_id, rec_bf);
                    }
                }
            });
            thread.start();
            threadList.add(thread);
        }

        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        String harden_method_str;
        if (hardening_method == null) harden_method_str = hardening_type;
        else harden_method_str = hardening_method.type;
        System.out.printf("Generated %d Bloom filters by %s & %s-%d & %s%n", rec_rbf_bf_dict.size(), encoding_method.type,
                hashing_method.type, hashing_method.num_hash_function, harden_method_str);
        return rec_rbf_bf_dict;
    }

    public Map<String, List<String>> gen_q_gram_dict(Map<String, List<String>> rec_attr_val_dict) {
        Map<String, List<String>> rec_q_gram_dict = new HashMap<>();  // The dictionary of q-gram lists.

        for (String entity_id : rec_attr_val_dict.keySet()) {
            List<String> rec_val_list = rec_attr_val_dict.get(entity_id);
            List<String> rec_q_gram_list = this.encoding_method.qGramListForRec(rec_val_list);
            rec_q_gram_dict.put(entity_id, rec_q_gram_list);
        }

        System.out.printf("Generated %d q-gram lists%n", rec_q_gram_dict.size());

        return rec_q_gram_dict;
    }

    public double cal_bf_sim(BitSet bf1, BitSet bf2) {
        assert bf1.size() == bf2.size();

        int num_ones_bf1 = bf1.cardinality();
        int num_ones_bf2 = bf2.cardinality();

        bf1.and(bf2);
        int num_common_ones = bf1.cardinality();

        return (2.0 * num_common_ones) / (num_ones_bf1 + num_ones_bf2);
    }

    public List<List<Integer>> init_minhash(int lsh_band_size, int lsh_num_band) {
        //        Initialise the parameters for Min-Hash Locality Sensitive Hashing (LSH)
        //       including generating random values for hash functions.

        /*LSH min-hashing follows the code provided here:
        https://github.com/chrisjmccormick/MinHash/blob/master/ \
                 runMinHashExample.py

        The probability for a pair of sets with Jaccard sim 0 < s <= 1 to be
        included as a candidate pair is (with b = lsh_num_band and
        r = lsh_band_size, i.e. the number of rows/hash functions per band) is
        (Leskovek et al., 2014):

        p_cand = 1- (1 - s^r)^b

        Approximation of the "threshold" of the S-curve (Leskovek et al., 2014)
        is: t = (1/k)^(1/r)

        If a string is given as plot_file_name then a graph of the probabilities
        will be generated and saved into this file.*/


        // Calculate error probabilities for given parameter values
        //
        assert lsh_num_band > 1;
        assert lsh_band_size > 1;

        int num_hash_funct = lsh_band_size * lsh_num_band;  // Total number needed

        double t = Math.pow(1.0 / (double) lsh_num_band, 1.0 / (double) lsh_band_size);

        List<double[]> s_p_cand_list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            double s = 0.1 * i;
            double p_cand = 1.0 - Math.pow((1.0 - Math.pow(s, lsh_band_size)), lsh_num_band);
            s_p_cand_list.add(new double[]{s, p_cand});
        }

        System.out.println("Initialise LSH blocking using Min-Hash");
        System.out.printf("  Number of hash functions: %d%n", num_hash_funct);
        System.out.printf("  Number of bands:          %d%n", lsh_num_band);
        System.out.printf("  Size of bands:            %d%n", lsh_band_size);
        System.out.printf("  Threshold of s-curve:     %.3f%n", t);
        System.out.println("  Probabilities for candidate pairs:");
        System.out.println("   Jacc_sim | prob(cand)");
        for (double[] s_p : s_p_cand_list) {
            System.out.printf("     %.2f   |   %.5f%n", s_p[0], s_p[1]);
        }

        double max_hash_val = Math.pow(2, 31) - 1;  // Maximum possible value a CRC hash could have

        // Random hash function will take the form of: h(x) = (a*x + b) % c
        // where "x" is the input value, "a" and "b" are random
        // coefficients, and
        // "c" is a prime number just greater than max_hash_val
        //
        // Generate "num_hash_funct" coefficients
        //
        Set<Integer> coeff_a_set = new HashSet<>();
        Set<Integer> coeff_b_set = new HashSet<>();

        Random random = new Random();
        random.setSeed(42);
        while (coeff_a_set.size() < num_hash_funct) {
            coeff_a_set.add(random.nextInt((int) max_hash_val));
        }
        while (coeff_b_set.size() < num_hash_funct) {
            coeff_b_set.add(random.nextInt((int) max_hash_val));
        }

        List<List<Integer>> result = new ArrayList<>();
        result.add(coeff_a_set.stream().toList());
        result.add(coeff_b_set.stream().toList());

        return result;
    }

    public List<List<Integer>> minhash_q_gram_set(List<String> q_gram_list, List<Integer> coeff_a_list,
                                                  List<Integer> coeff_b_list, int lsh_band_size, int lsh_num_band) {
        //Min-hash the given set of q-grams and return a list of hash signatures
        //depending upon the Min-hash parameters set with the "init_minhash"
        //method.

        // We need the next largest prime number above "maxShingleID".
        // From here:
        // http://compoasso.free.fr/primelistweb/page/prime/liste_online_en.php
        //
        long next_prime = 4294967311L;

        Set<Integer> crc_hash_set = new HashSet<>();

        Set<String> q_gram_set = new HashSet<>(q_gram_list);
        CRC32 crc32 = new CRC32();
        for (String q_gram : q_gram_set) {
            // Hash the q-grams into 32-bit integers
            crc32.update(q_gram.getBytes(StandardCharsets.UTF_8));
            crc_hash_set.add((int) crc32.getValue());
        }

        assert q_gram_set.size() == crc_hash_set.size();  // Check no collision

        // Now generate all the min-hash values for this q-gram set
        //
        List<Integer> min_hash_sig_list = new ArrayList<>();
        int num_hash_funct = lsh_band_size * lsh_num_band;
        for (int i = 0; i < num_hash_funct; i++) {
            // For each CRC hash value (q-gram) in the q-gram set calculate
            // its Min-hash value for all "num_hash_funct" functions
            //
            long min_hash_val = next_prime + 1; // Initialise to value outside range

            for (int crc_hash_val : crc_hash_set) {
                int hash_val = (int) (((long) coeff_a_list.get(i) * crc_hash_val + coeff_b_list.get(i)) % next_prime);
                min_hash_val = Math.min(min_hash_val, hash_val);
                min_hash_sig_list.add((int) min_hash_val);
            }
        }
        // Now split hash values into bands and generate the list of
        // "lsh_num_band" hash values used for blocking
        //
        List<List<Integer>> band_hash_sig_list = new ArrayList<>();

        int start_ind = 0;
        int end_ind = lsh_band_size;

        for (int band_num = 0; band_num < lsh_num_band; band_num++) {
            List<Integer> band_hash_sig = min_hash_sig_list.subList(start_ind, end_ind);
            assert band_hash_sig.size() == lsh_band_size;

            start_ind = end_ind;
            end_ind += lsh_band_size;
            band_hash_sig_list.add(band_hash_sig);
        }
        return band_hash_sig_list;
    }

    public Map<String, Set<String>> minhash_blocking(Map<String, List<String>> q_gram_list_dict,
                                                     List<Integer> coeff_a_list, List<Integer> coeff_b_list,
                                                     int lsh_band_size, int lsh_num_band) {
        System.out.println("Conduct Min-hash LSH blocking");

        Date time = new Date();
        long start_time = time.getTime();

        Map<String, Set<String>> minhash_dict = new HashMap<>(); // Keys will be min-hash signatures, values set of identifiers

        int num_empty_q_gram_set = 0;
        int num_rec_hashed = 0;

        // Loop over all individuals, and if their certificate and role types
        // fulfill one of the given ones hash the individual"s record
        //
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (String entity_id : q_gram_list_dict.keySet()) {
            List<String> q_gram_list = q_gram_list_dict.get(entity_id);
            num_rec_hashed += 1;

            if (q_gram_list.size() == 0) {
                num_empty_q_gram_set += 1;
                continue;  // Do not index empty q-gram set
            }

            // Get the min-hash signatures of this q-gram set
            //
            List<List<Integer>> band_hash_sig_list = minhash_q_gram_set(q_gram_list, coeff_a_list,
                    coeff_b_list, lsh_band_size, lsh_num_band);

            assert band_hash_sig_list.size() == lsh_num_band;

            // Insert each individual into blocks according to its min-hash values
            //
            for (List<Integer> band_hash_sig : band_hash_sig_list) {
                // To save memory convert into a MD5 hashes
                //
                assert messageDigest != null;
                messageDigest.update(list2String(band_hash_sig).getBytes());
                byte[] bytes = messageDigest.digest();
                String block_key_val = new BigInteger(1, bytes).toString(16);
                // block_key_val = str(band_hash_sig)

                Set<String> block_rec_id_set = minhash_dict.getOrDefault(block_key_val, new HashSet<>());
                block_rec_id_set.add(entity_id);
                minhash_dict.put(block_key_val, block_rec_id_set);
            }
        }


        assert q_gram_list_dict.size() == num_rec_hashed;

        System.out.printf("  Hashed %d records in %d msec%n", num_rec_hashed, new Date().getTime() - start_time);

        System.out.printf("    Inserted each record into %d blocks%n", lsh_num_band);

        System.out.printf("      Number of empty attribute value q-gram sets: %d%n", num_empty_q_gram_set);

        // Calculate statistics of generated blocks
        //
        statistic_block(minhash_dict);

        return minhash_dict;
    }

    public Map<String, Set<String>> hlsh_blocking(Map<String, BitSet> rec_bf_dict, int num_seg) {
        assert num_seg >= 1;

        Map<String, Set<String>> block_dict = new HashMap<>();

        int bf_len;
        // Loop over all records and extract all Bloom filter bit position sub arrays
        // of length "num_bit_pos_key" and insert the record into these corresponding
        // blocks
        //
        for (String entity_id : rec_bf_dict.keySet()) {
            BitSet rec_bf = rec_bf_dict.get(entity_id);
            // First time calculate the indices to use for splitting a Bloom filter
            //
            bf_len = rec_bf.size();

            int seg_len = bf_len / num_seg;

            List<int[]> bf_split_index_list = new ArrayList<>();
            int start_pos = 0;
            int end_pos = seg_len;
            while (end_pos <= bf_len) {
                bf_split_index_list.add(new int[]{start_pos, end_pos});
                start_pos = end_pos;
                end_pos += seg_len;
            }
            // Depending upon the Bloom filter length and "num_bit_pos_key" to use
            // the last segement might contain less than "num_bit_pos_key" positions.

            // Extract the bit position arrays for these segments
            //
            for (int[] pos_pair : bf_split_index_list) {
                BitSet bf_seg = rec_bf.get(pos_pair[0], pos_pair[1]);

                String block_key = bf_seg.toString();  // Make it a string
                Set<String> block_ent_id_set = block_dict.getOrDefault(block_key, new HashSet<>());
                block_ent_id_set.add(entity_id);
                block_dict.put(block_key, block_ent_id_set);
            }
        }

        System.out.printf("Bloom filter HLSH blocking dictionary contains %d blocks (with %d segments)%n",
                block_dict.keySet().size(), num_seg);
        statistic_block(block_dict);

        return block_dict;
    }

    private void statistic_block(Map<String, Set<String>> block_dict) {
        // Calculate statistics of generated blocks
        //
        List<Integer> block_size_list = new ArrayList<>();
        int num_pair_comp = 0; // How many comparisons to be done with this blocking
        int num_block_size1 = 0; // Number of blocks with only one individual
        List<Integer> all_block_size_list = new ArrayList<>();
        List<String> block_delete_list = new ArrayList<>();
        int num_large_block_del = 0;

        int sum_all_block_size = 0;
        int min_all_block_size = 100;
        int max_all_block_size = 0;
        for (String block_key : block_dict.keySet()) {
            Set<String> block_ent_id_set = block_dict.get(block_key);
            int num_rec_in_block = block_ent_id_set.size();
            all_block_size_list.add(num_rec_in_block);
            if (num_rec_in_block > MAX_BLOCK_SIZE) {
                block_delete_list.add(block_key);
                num_large_block_del += 1;
            } else block_size_list.add(num_rec_in_block);
            if (num_rec_in_block == 1) num_block_size1 += 1;
            sum_all_block_size += num_rec_in_block;
            if (max_all_block_size <= num_rec_in_block) max_all_block_size = num_rec_in_block;
            if (min_all_block_size >= num_rec_in_block) min_all_block_size = num_rec_in_block;
        }


        for (String block_key : block_delete_list) {
            block_dict.remove(block_key);
        }

        System.out.printf("  Minimum, average and maximum block sizes (all blocks): %d / %.2f / %d%n", min_all_block_size,
                (double) sum_all_block_size / all_block_size_list.size(), max_all_block_size);
        System.out.printf("    %d block only contain 1 entity%n", num_block_size1);
        System.out.printf("    Removed %d blocks larger than %d records, %d blocks left%n", num_large_block_del,
                MAX_BLOCK_SIZE, block_size_list.size());
        if (block_size_list.size() == 0) {
            System.out.println("    Warning: No blocks left");
        }
    }

    public Map<String[], Double> conduct_q_gram_linkage(Map<String, List<String>> val_dict1,
                                                        Map<String, Set<String>> block_dict1,
                                                        Map<String, List<String>> val_dict2,
                                                        Map<String, Set<String>> block_dict2, double min_sim) {
        assert min_sim >= 0.0;
        assert min_sim <= 1.0;
        Map<String[], Double> rec_pair_dict = new HashMap<>();

        // Keep track of pairs compared so each pair is only compared once
        //
        Set<String[]> pairs_compared_set = new HashSet<>();

        // print("Similarity threshold based classification")
        // print("  Minimum similarity of record pairs to be stored: %.2f" % (min_sim))

        // Iterate over all block values that occur in both data sets
        //
        for (String block_val1 : block_dict1.keySet()) {
            if (!block_dict2.containsKey(block_val1))
                continue; // Block value not in common, go to next one

            Set<String> entity_id_set1 = block_dict1.get(block_val1);
            Set<String> entity_id_set2 = block_dict2.get(block_val1);

            // Iterate over all value pairs
            //
            for (String entity_id1 : entity_id_set1) {
                List<String> val1 = val_dict1.get(entity_id1);

                for (String entity_id2 : entity_id_set2) {
                    String[] entity_id_pair = new String[2];
                    if (entity_id1.compareTo(entity_id2) >= 0) {
                        entity_id_pair[0] = entity_id1;
                        entity_id_pair[1] = entity_id2;
                    } else {
                        entity_id_pair[0] = entity_id2;
                        entity_id_pair[1] = entity_id1;
                    }

                    if (!pairs_compared_set.contains(entity_id_pair)) {
                        List<String> val2 = val_dict2.get(entity_id2);
                        pairs_compared_set.add(entity_id_pair);

                        double sim = hashing_method.cal_q_gram_sim(val1, val2); // Calculate the similarity

                        if (sim >= min_sim) rec_pair_dict.put(entity_id_pair, sim);
                    }
                }
            }
        }
        int num_all_comparisons = val_dict1.size() * val_dict2.size();
        int num_pairs_compared = pairs_compared_set.size();

        System.out.printf("Compared %d record q-gram pairs (full comparison is %d record pairs)%n",
                num_pairs_compared, num_all_comparisons);

        // print("  Reduction ratio: %2f" % (1.0 - float(num_pairs_compared) / num_all_comparisons))
        System.out.printf("  Stored %d record q-gram pairs with a similarity of at least %.2f%n", rec_pair_dict.size(), min_sim);

        pairs_compared_set.clear();

        return rec_pair_dict;
    }

    public Map<String[], Double> conduct_bf_linkage(Map<String, BitSet> val_dict1,
                                                    Map<String, Set<String>> block_dict1,
                                                    Map<String, BitSet> val_dict2,
                                                    Map<String, Set<String>> block_dict2, double min_sim) {
        assert min_sim >= 0.0;
        assert min_sim <= 1.0;
        Map<String[], Double> rec_pair_dict = new HashMap<>();

        // Keep track of pairs compared so each pair is only compared once
        //
        Set<String[]> pairs_compared_set = new HashSet<>();

        // print("Similarity threshold based classification")
        // print("  Minimum similarity of record pairs to be stored: %.2f" % (min_sim))

        // Iterate over all block values that occur in both data sets
        //
        for (String block_val1 : block_dict1.keySet()) {
            if (!block_dict2.containsKey(block_val1))
                continue; // Block value not in common, go to next one

            Set<String> entity_id_set1 = block_dict1.get(block_val1);
            Set<String> entity_id_set2 = block_dict2.get(block_val1);

            // Iterate over all value pairs
            //
            for (String entity_id1 : entity_id_set1) {
                BitSet val1 = val_dict1.get(entity_id1);

                for (String entity_id2 : entity_id_set2) {
                    String[] entity_id_pair = new String[2];
                    if (entity_id1.compareTo(entity_id2) >= 0) {
                        entity_id_pair[0] = entity_id1;
                        entity_id_pair[1] = entity_id2;
                    } else {
                        entity_id_pair[0] = entity_id2;
                        entity_id_pair[1] = entity_id1;
                    }

                    if (!pairs_compared_set.contains(entity_id_pair)) {
                        BitSet val2 = val_dict2.get(entity_id2);
                        pairs_compared_set.add(entity_id_pair);

                        double sim = cal_bf_sim(val1, val2); // Calculate the similarity

                        if (sim >= min_sim)
                            rec_pair_dict.put(entity_id_pair, sim);
                    }
                }
            }
        }
        int num_all_comparisons = val_dict1.size() * val_dict2.size();
        int num_pairs_compared = pairs_compared_set.size();

        System.out.printf("Compared %d record bf pairs (full comparison is %d record pairs)%n",
                num_pairs_compared, num_all_comparisons);

        System.out.printf("  Stored %d record bf pairs with a similarity of at least %.2f%n",
                rec_pair_dict.size(), min_sim);

        pairs_compared_set.clear();

        return rec_pair_dict;
    }

    public Map<Double, int[]> calc_linkage_outcomes(Map<String[], Double> rec_pair_dict, double[] sim_threshold_list) {
        Map<Double, int[]> class_res_dict = new HashMap<>();

        for (double sim_threshold : sim_threshold_list) {
            class_res_dict.put(sim_threshold, new int[]{0, 0, 0, 0});  // Initalise the results counters
        }
        boolean is_true_match;
        for (String[] entity_id_pair : rec_pair_dict.keySet()) {
            is_true_match = entity_id_pair[0].equals(entity_id_pair[1]);

            //  Calculate linkage results for all given similarities
            //
            double sim = rec_pair_dict.get(entity_id_pair);
            for (double sim_threshold : sim_threshold_list) {
                int[] sim_res_list = class_res_dict.get(sim_threshold);

                if (sim >= sim_threshold && is_true_match)  // TP
                    sim_res_list[0] += 1;
                else if (sim >= sim_threshold && !is_true_match) // FP
                    sim_res_list[1] += 1;
                else if (sim < sim_threshold && is_true_match)  // FN
                    sim_res_list[3] += 1;
                // TN are calculated at the end
            }
        }
        for (double sim_threshold : sim_threshold_list) {
            int[] sim_res_list = class_res_dict.get(sim_threshold);

            // Calculate the number of TN as all comparisons - (TP + FP + FN)
            //
            sim_res_list[2] = rec_pair_dict.keySet().size() - Arrays.stream(sim_res_list).sum();
        }
        return class_res_dict;
    }

    public double calc_precision(int num_tp, int num_fp) {
        double precision;
        if ((num_tp + num_fp) > 0)
            precision = (double) num_tp / (num_tp + num_fp);
        else precision = 0.0;

        return Double.parseDouble(String.format("%.4f", precision));
    }

    public double calc_recall(int num_tp, int num_fn) {
        double recall;
        if ((num_tp + num_fn) > 0)
            recall = (double) num_tp / (num_tp + num_fn);
        else recall = 0.0;

        return Double.parseDouble(String.format("%.4f", recall));
    }


    public int cal_opt_k(Map<String, List<String>> rec_q_gram_dict1, Map<String, List<String>> rec_q_gram_dict2,
                         int bf_len) {
        // Get the average number of q-grams in the attribute values
        //
        double total_num_q_gram = 0.0;
        int total_num_val = 0;

        // Calculate average number of q-grams of all attribute values
        //
        List<String> rec_q_grams;
        for (String entity_id : rec_q_gram_dict1.keySet()) {
            rec_q_grams = rec_q_gram_dict1.get(entity_id);
            total_num_q_gram += rec_q_grams.size();

            total_num_val += 1;
        }

        for (String entity_id : rec_q_gram_dict2.keySet()) {
            rec_q_grams = rec_q_gram_dict2.get(entity_id);
            total_num_q_gram += rec_q_grams.size();

            total_num_val += 1;
        }

        double avrg_num_q_gram = total_num_q_gram / total_num_val;

        // Set number of hash functions to have in average 50% of bits set to 1
        //
        // For RBF we take the average number of q-grams across all attributes
        //
        // if ("rbf-s" in encode_type_list or "rbf-d" in encode_type_list):
        //  avrg_num_q_gram = sum(attr_avrg_num_q_gram_list) / \
        //                    len(attr_avrg_num_q_gram_list)

        return (int) (Math.round(Math.log(2.0) * bf_len / avrg_num_q_gram));
    }

    public String list2String(List<Integer> list) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int item : list) {
            stringBuilder.append(item).append(',');
        }
        return stringBuilder.toString();
    }

    public void storeResult(String filename, List<Double> bfPrecisions, List<Double> bfRecalls) {
        File file = new File(filename);
        File filePath = new File(file.getParent());
        if (!filePath.exists()) {
            if (!filePath.mkdirs()) {
                System.out.printf("创建文件夹%s失败\n", filePath.getPath());
                return;
            }
        }

        try {
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    System.out.printf("创建文件%s失败\n", filename);
                    return;
                }
            }
            FileWriter fw = new FileWriter(file, false);
            BufferedWriter bw = new BufferedWriter(fw);
            String line = "precision,recall";
            bw.write(line + "\n");
            for (int i = 0;i < bfPrecisions.size();i++){
                line = bfPrecisions.get(i) + "," + bfRecalls.get(i);
                bw.write(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        String res_plot_file_name = "/Users/takafumikai/PycharmProjects/LDP-BV/result/result-auc.png";
        String data_set_file_name1 = "/Users/takafumikai/PycharmProjects/LDP-BV/dataset/ncvoter/ncvoter-50000-1.csv";
        String data_set_file_name2 = "/Users/takafumikai/PycharmProjects/LDP-BV/dataset/ncvoter/ncvoter-50000-2.csv";
        int[] attr_index_list = {1, 2, 4, 5};  // attr's columns used
        int entity_id_col = 0;
        double[] sim_threshold_list = {0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0};
        String block_method = "minhash-32-5";
        int q = 2;
        boolean padded = false;
        int bf_len = 1000;
        String hash_type = "dh-True";
        String num_hash_functions = "10";
        String[] encode_type_list = {"clk"};
        String[] bf_harden_types = new String[]{"rexor"};

        for (double sim_threshold : sim_threshold_list) {
            assert 0.0 <= sim_threshold;
            assert sim_threshold <= 1.0;
        }

        int block_hlsh_num_seg = 0;
        int lsh_band_size = 0;
        int lsh_num_band = 0;
        if (!block_method.equals("none")) {
            if (block_method.startsWith("hlsh"))
                block_hlsh_num_seg = Integer.parseInt(block_method.split("-")[1]);
            if (block_method.startsWith("minhash")) {
                lsh_band_size = Integer.parseInt(block_method.split("-")[2]);
                lsh_num_band = Integer.parseInt(block_method.split("-")[1]);
            }
        }

        boolean appendCntFlag = hash_type.split("-")[1].toLowerCase(Locale.ROOT).equals("true");
        hash_type = hash_type.split("-")[0].toLowerCase(Locale.ROOT);

//        int mc_chain_len = 0;
//        String mc_sel_method = null;
        int salt_attr_index = -1; // for salt
        int w_size = 0; // for wxor
        double flip_prob = 0; // for blip
        double epsilon = 0; // for urap
        double ratio = 0; // for urap, urap-prob
        List<String> epsilons = new ArrayList<>(); // for id-ldp

        // Define three hash functions
        //
        String bf_hash_function1 = "SHA1";
        String bf_hash_function2 = "MD5";
        String bf_hash_function3 = "SHA2";

        // Load the two data sets
        //
        EvalLinkage evalLinkage = new EvalLinkage(null, null, null);
        List<Map> result1 = evalLinkage.load_dataset_salt(data_set_file_name1, attr_index_list, entity_id_col, salt_attr_index);
        Map<String, List<String>> rec_attr_val_dict1 = result1.get(0);
        Map<String, String> salt_dict1 = result1.get(1);

        List<Map> result2 = evalLinkage.load_dataset_salt(data_set_file_name2, attr_index_list, entity_id_col, salt_attr_index);
        Map<String, List<String>> rec_attr_val_dict2 = result2.get(0);
        Map<String, String> salt_dict2 = result2.get(1);

        // Combine into one list for later use
        //
        List<List<String>> all_rec_list = new ArrayList<>();
        for (String entity_id : rec_attr_val_dict1.keySet()) {
            all_rec_list.add(rec_attr_val_dict1.get(entity_id));
        }
        for (String entity_id : rec_attr_val_dict2.keySet()) {
            all_rec_list.add(rec_attr_val_dict2.get(entity_id));
        }

        // Initialise the hashing method
        //
        Hashing hash_method = switch (hash_type) {
            case "dh" -> new DoubleHashing("dh", bf_hash_function1, bf_hash_function2, bf_len, 0);
            case "rh" -> new RandomHashing("rh", bf_hash_function1, bf_len, 0);
            case "edh" -> new EnhancedDoubleHashing("edh", bf_hash_function1, bf_hash_function2, bf_len, 0);
            default -> null;
        };
        evalLinkage.hashing_method = hash_method; // 赋值给EvalLinkage

        int times = 0;// the times do linkage, when i == 0, do block and q_gram sim

        // Store precision and recall result
        List<Double> qgramPrecisions = new ArrayList<>();
        List<Double> qgramRecalls = new ArrayList<>();
        List<List<Double>> enc_prec_list = new ArrayList<>();
        List<List<Double>> enc_reca_list = new ArrayList<>();

        // Initialize the legend list
        List<String> legend_str_list = new ArrayList<>();
        legend_str_list.add("Q-Gram");

        //Initialize
        Map<String, Set<String>> block_dict1 = null;
        Map<String, Set<String>> block_dict2 = null;
        Map<String, List<String>> rec_q_gram_dict1 = null;
        Map<String, List<String>> rec_q_gram_dict2 = null;

        for (String encode_type : encode_type_list) {
            // TODO 添加MC
            for (String bf_harden_type : bf_harden_types) {
                if (bf_harden_type.equals("salt")) {
//            salt_attr_index = Integer.parseInt(sys.argv[15]);
                    salt_attr_index = 6;
                } else if (bf_harden_type.startsWith("wxor")) {
                    w_size = Integer.parseInt(bf_harden_type.split("-")[1]);
                } else if (bf_harden_type.startsWith("blip")) {
                    flip_prob = Double.parseDouble(bf_harden_type.split("-")[1]);
                    // random_choice = True
                } else if (bf_harden_type.startsWith("urap")) {
                    epsilon = Double.parseDouble(bf_harden_type.split("-")[2]);
                    ratio = Double.parseDouble(bf_harden_type.split("-")[1]);
                } else if (bf_harden_type.startsWith("indexd")) {
                    // epsilon will be turned to ln(epsilon) in harden method
                    epsilons = new ArrayList<>(Arrays.asList(bf_harden_type.split("-")[1]));
                }

                Encoding encode_method = null;
                if (encode_type.equals("clk")) {
                    encode_method = new CLKBFEncoding("CLK", q, padded, hash_method, appendCntFlag);
                } else if (encode_type.startsWith("rbf")) {
                    // Calculate number of bits to be sampled as total Bloom filter length
                    // divided by the number of attributes
                    //
                    int num_bf_bit = bf_len / attr_index_list.length;
                    encode_method = new RecordBFEncoding(encode_type.toUpperCase(Locale.ROOT), q, padded, hash_method, appendCntFlag);
                    for (int index : attr_index_list) {
                        //TODO Dynamic ABF length
                        encode_method.set_attr_param_list(q, padded, num_bf_bit, appendCntFlag);
                    }
                }
                evalLinkage.encoding_method = encode_method; // 赋值给EvalLinkage

                // only generate q-gram list \ k \ harden_method once
                Hardening harden_method = null;
                if (times == 0) {
                    // Generate q-grams for the attribute value lists and add into a
                    // dictionary for data set1
                    //
                    rec_q_gram_dict1 = evalLinkage.gen_q_gram_dict(rec_attr_val_dict1);
                    // Generate q-grams for the attribute value lists and add into a
                    // dictionary for data set2
                    //
                    rec_q_gram_dict2 = evalLinkage.gen_q_gram_dict(rec_attr_val_dict2);

                    // Set num of hash functions
                    if (num_hash_functions.equals("opt")) {
                        evalLinkage.hashing_method.num_hash_function = evalLinkage.cal_opt_k(rec_q_gram_dict1, rec_q_gram_dict2, bf_len);
                    } else {
                        evalLinkage.hashing_method.num_hash_function = Integer.parseInt(num_hash_functions);
                    }

                    // Initalise the hardening method if needed
                    //
                    if (bf_harden_type.equals("none"))
                        harden_method = null;
                    else if (bf_harden_type.equals("bal")) {
                        harden_method = new Balancing("Balancing", bf_len);
                        bf_len *= 2;
                    } else if (bf_harden_type.equals("xor")) {
                        harden_method = new XorFolding("Xor-fold");
                        bf_len /= 2;
                    } else if (bf_harden_type.equals("r90")) {
                        harden_method = new Rule90("Rule 90");
                    } else if (bf_harden_type.startsWith("blip")) {
                        harden_method = new Blip(String.format("Blip %.2f", flip_prob), flip_prob);
                    } else if (bf_harden_type.equals("salt")) {
                        harden_method = null;  // To indicate salting is to be applied
                    }
                    //TODO MarkovChain Harden
                    else if (bf_harden_type.startsWith("wxor")) {
                        harden_method = new Wxor(String.format("WXOR %d", w_size), w_size);
                    } else if (bf_harden_type.equals("rexor")) {
                        harden_method = new ResamXor("REXOR", bf_len);
                    } else if (bf_harden_type.startsWith("urap")) {
                        harden_method = new Urap(String.format("Urap %.2f %.2f", ratio, epsilon), ratio, epsilon,
                                data_set_file_name1, encode_method.type, hash_type, q, padded,
                                num_hash_functions, bf_len);
                    } else if (bf_harden_type.startsWith("indexD")) {
                        harden_method = new IndexD(bf_harden_type, epsilons,
                                data_set_file_name1, encode_method.type, hash_type, q, padded,
                                num_hash_functions, bf_len);
                    }
                    evalLinkage.hardening_method = harden_method; // 赋值给EvalLinkage
                }

                // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                // Encode data sets into Bloom filters
                //

                long start_time = new Date().getTime();
                Map<String, BitSet> rec_bf_dict1;
                Map<String, BitSet> rec_bf_dict2;
                if (encode_type.equals("clk")) {
                    rec_bf_dict1 = evalLinkage.gen_clk_bf_dict(rec_q_gram_dict1, salt_dict1, bf_harden_type, bf_len);
                    rec_bf_dict2 = evalLinkage.gen_clk_bf_dict(rec_q_gram_dict1, salt_dict2, bf_harden_type, bf_len);
                } else {
                    rec_bf_dict1 = evalLinkage.gen_rbf_bf_dict(rec_attr_val_dict1, salt_dict1, bf_harden_type, bf_len);
                    rec_bf_dict2 = evalLinkage.gen_rbf_bf_dict(rec_attr_val_dict1, salt_dict2, bf_harden_type, bf_len);
                }

                System.out.printf("Time used for generating bf dict:         %d msec%n", new Date().getTime() - start_time);

                // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                // Perform the linkage (possibly do blocking first)
                //
                start_time = new Date().getTime();

                if (block_method.equals("none")) {
                    if (times == 0) {
                        // Generate one blocking value per data set (so all records in one block)
                        //
                        block_dict1 = new HashMap<>();
                        block_dict1.put("all", rec_q_gram_dict1.keySet());
                        block_dict2 = new HashMap<>();
                        block_dict2.put("all", rec_q_gram_dict2.keySet());
                    }
                } else {
                    if (block_method.startsWith("minhash")) {
                        if (times == 0) {
                            // Initialise min-hash parameters
                            // -------------------------------
                            // lsh_band_size = 5
                            // lsh_num_band  = 32
                            // -------------------------------
                            List<List<Integer>> result = evalLinkage.init_minhash(lsh_band_size, lsh_num_band);
                            List<Integer> coeff_a_list = result.get(0);
                            List<Integer> coeff_b_list = result.get(1);

                            // Min-hash based blocking
                            block_dict1 = evalLinkage.minhash_blocking(rec_q_gram_dict1, coeff_a_list,
                                    coeff_b_list, lsh_band_size, lsh_num_band);
                            block_dict2 = evalLinkage.minhash_blocking(rec_q_gram_dict2, coeff_a_list,
                                    coeff_b_list, lsh_band_size, lsh_num_band);
                        }
                    } else {
                        if (times == 0) {
                            block_dict1 = evalLinkage.hlsh_blocking(rec_bf_dict1, block_hlsh_num_seg);
                            block_dict2 = evalLinkage.hlsh_blocking(rec_bf_dict2, block_hlsh_num_seg);
                        }
                    }
                }

                double min_sim = sim_threshold_list[0];

                long blocking_time = new Date().getTime() - start_time;

                // pais-completeness
                System.out.printf("Time used for blocking:             %d msec%n", blocking_time);

                // Perform q-gram based and Bloom filter linkage
                //
                start_time = new Date().getTime();
                Map<String[], Double> q_gram_rec_pair_dict;
                long q_gram_linkage_time = 0;
                Map<Double, int[]> q_gram_class_res_dict = null;
                if (times == 0) {
                    q_gram_rec_pair_dict = evalLinkage.conduct_q_gram_linkage(rec_q_gram_dict1, block_dict1,
                            rec_q_gram_dict2, block_dict2, min_sim);

                    q_gram_linkage_time = new Date().getTime() - start_time;
                    q_gram_class_res_dict = evalLinkage.calc_linkage_outcomes(q_gram_rec_pair_dict, sim_threshold_list);
                }

                start_time = new Date().getTime();

                assert block_dict1 != null;
                assert block_dict2 != null;
                Map<String[], Double> bf_rec_pair_dict = evalLinkage.conduct_bf_linkage(rec_bf_dict1, block_dict1,
                        rec_bf_dict2, block_dict2, min_sim);

                long bf_linkage_time = new Date().getTime() - start_time;
                Map<Double, int[]> bf_class_res_dict = evalLinkage.calc_linkage_outcomes(bf_rec_pair_dict, sim_threshold_list);

                System.out.printf("\nEncoding method used:               %s%n", encode_type);

                System.out.printf("Time used for q-gram linkage:       %d msec%n", q_gram_linkage_time);

                System.out.printf("Time used for Bloom filter linkage: %d msec%n", bf_linkage_time);

                // Calulate precision and recall values for the different thresholds
                //
                int[] tp_fp_tn_fn;
                int tp;
                int fp;
                int tn;
                int fn;
                double precision;
                double recall;
                if (times == 0) {
                    for (double sim_threshold : sim_threshold_list) {
                        tp_fp_tn_fn = q_gram_class_res_dict.get(sim_threshold);
                        tp = tp_fp_tn_fn[0];
                        fp = tp_fp_tn_fn[1];
                        fn = tp_fp_tn_fn[3];
                        precision = evalLinkage.calc_precision(tp, fp);
                        qgramPrecisions.add(precision);
                        recall = evalLinkage.calc_recall(tp, fn);
                        qgramRecalls.add(recall);
                    }

                    String qgramResultFilePath = new File(data_set_file_name1).getParent();
                    String qgramResultFilename = String.format("q%d-padded%s-appendCntFlag%s.csv", q, padded, appendCntFlag);
                    String qgramResultFile = qgramResultFilePath + "/result/" + qgramResultFilename;
                    evalLinkage.storeResult(qgramResultFile, qgramPrecisions, qgramRecalls);
                }
                List<Double> bfPrecisions = new ArrayList<>();
                List<Double> bfRecalls = new ArrayList<>();

                for (double sim_threshold : sim_threshold_list) {
                    tp_fp_tn_fn = bf_class_res_dict.get(sim_threshold);
                    tp = tp_fp_tn_fn[0];
                    fp = tp_fp_tn_fn[1];
                    fn = tp_fp_tn_fn[3];
                    precision = evalLinkage.calc_precision(tp, fp);
                    bfPrecisions.add(precision);
                    recall = evalLinkage.calc_recall(tp, fn);
                    bfRecalls.add(recall);
                }
                System.out.printf("similarity threshold: %s%n", Arrays.toString(sim_threshold_list));
                System.out.printf("q prec: %s%n", qgramPrecisions);
                System.out.printf("q reca: %s%n", qgramRecalls);
                System.out.printf("bf prec:%s%n", bfPrecisions);
                System.out.printf("bf reca: %s%n\n", bfRecalls);

                enc_prec_list.add(bfPrecisions);
                enc_reca_list.add(bfRecalls);

                times += 1;

                // Print a line with summary memory and timing results
//
                System.out.printf("Result saved in %s, block time: %d, q-gram link time: %d, bf link time: %d %n",
                        res_plot_file_name, blocking_time, q_gram_linkage_time, bf_linkage_time);

                if (bf_harden_type.equals("bal")) bf_len = bf_len / 2;
                if (bf_harden_type.equals("xor")) bf_len = bf_len * 2;

                legend_str_list.add(encode_type.toUpperCase(Locale.ROOT));

                String resultFilePath = new File(data_set_file_name1).getParent();
                String resultFilename = String.format("%s-q%d-k%s-bflen%d.csv", bf_harden_type, q, num_hash_functions, bf_len);
                String resultFile = resultFilePath + String.format("/result/%s/%s/",
                        encode_type.toUpperCase(Locale.ROOT), hash_type.toUpperCase(Locale.ROOT)) + resultFilename;
                evalLinkage.storeResult(resultFile, bfPrecisions, bfRecalls);
            }
            //TODO 画图
//            draw_plot_auc(qgramPrecisions, qgramRecalls, enc_prec_list, enc_reca_list,
//                    "Precision","Recall",legend_str_list, param_plot_title_str,
//                    title2="", save_fig_name=res_plot_file_name,
//                    aspect_ratio=PLOT_RATIO)
//
//            draw_plot_precision_recall(qgramPrecisions, qgramRecalls, enc_prec_list, enc_reca_list, sim_threshold_list,
//                    legend_str_list, param_plot_title_str,
//                    save_fig_name=res_plot_file_name.replace("auc", "split"),aspect_ratio=PLOT_RATIO)
        }
    }
}
