package String;

import java.util.*;

public class Encoding {
    public final String PAD_CHAR = "_";  // Used for q-gram pad
    public String type;
    public Hashing hash_method;
    public int q;
    public boolean padded;
    public boolean appendCntFlag;

    public Encoding(String type, Hashing hash_method, int q, boolean padded, boolean appendCntFlag) {
        this.type = type;
        this.hash_method = hash_method;
        this.q = q;
        this.padded = padded;
        this.appendCntFlag = appendCntFlag;
    }

    public boolean isNumeric(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public List<String> qGramListForRec(List<String> attr_val_list) {

        List<String> qGramList = new ArrayList<>();
        for (String attr_val : attr_val_list) {
            qGramList.addAll(qGramListForAttr(attr_val, q, padded));
        }

        return qGramList;
    }

    public List<String> qGramListForAttr(String attr_val, int q, boolean padded) {

        List<String> qGramList = new ArrayList<>();
        int padded_num = q - 1;
        String qGram;
        if (isNumeric(attr_val)) {
            for (int j = 0; j < attr_val.length(); j++) {
                qGram = String.valueOf(attr_val.charAt(j));
                qGramList.add(qGram);
            }
        } else {
            if (padded) {
                attr_val = this.PAD_CHAR.repeat(Math.max(0, padded_num)) + attr_val
                        + this.PAD_CHAR.repeat(Math.max(0, padded_num));
            }
            for (int j = 0; j < attr_val.length() - q + 1; j++) {
                qGram = attr_val.substring(j, j + q);
                qGramList.add(qGram);
            }
        }

        return qGramList;
    }


    void add_count_2_q_gram(List<String> q_gram_list) {
        Map<String, Integer> qGramCountMap = new HashMap<>();
        int count;
        for (String qGram : q_gram_list) {
            count = qGramCountMap.getOrDefault(qGram, 0);
            qGram = qGram + (count + 1);
            qGramCountMap.put(qGram, count + 1);
        }
    }


    public BitSet clk_encode(List<String> q_gram_list) {
        return null;
    }

    public BitSet clk_encode(List<String> q_gram_list, boolean do_mc) {
        return null;
    }

    public BitSet clk_encode(List<String> q_gram_list, String salt) {
        return null;
    }

    public BitSet rbf_encode(List<String> q_gram_list, int bf_len) {
        return null;
    }

    public BitSet rbf_encode(List<String> q_gram_list, int bf_len, boolean do_mc) {
        return null;
    }

    public BitSet rbf_encode(List<String> rec_attr_val_list, String salt, int bf_len) {
        return null;
    }

    public void set_attr_param_list(int q, boolean padded, int bf_len, boolean deal_dup) {
    }
}

class CLKBFEncoding extends Encoding {

    CLKBFEncoding(String type, int q, boolean padded, Hashing hash_method, boolean deal_dup) {
        super(type, hash_method, q, padded, deal_dup);
    }

    @Override
    public BitSet clk_encode(List<String> q_gram_list) {
        BitSet clk_bf;

        if (appendCntFlag) add_count_2_q_gram(q_gram_list);

        clk_bf = this.hash_method.hash_q_gram_list(q_gram_list);

        return clk_bf;
    }

    @Override
    public BitSet clk_encode(List<String> q_gram_list, boolean do_mc) {
        BitSet clk_bf;

        //TODO MC方法encoding
//            if mc_harden_class is not None:
//            q_gram_list_mc = []
//            if attr_val.isdigit():
//            q_gram_list_mc.extend([attr_val[i] for i in range(attr_val_len)])
//            else:
//            q_gram_list_mc.extend([attr_val[i:i + this.q] for i in range(attr_val_len - padded_num)])
//            extra_q_gram_set = mc_harden_class.get_other_q_grams_from_lang_model(q_gram_list_mc)
//            q_gram_list.extend(extra_q_gram_set)
        if (appendCntFlag) add_count_2_q_gram(q_gram_list);

        clk_bf = this.hash_method.hash_q_gram_list(q_gram_list);

        return clk_bf;
    }

    @Override
    public BitSet clk_encode(List<String> q_gram_list, String salt) {
        BitSet clk_bf;

        for (String qGram : q_gram_list) {
            qGram = qGram + salt;
        }

        return clk_encode(q_gram_list);
    }
}

class RecordBFEncoding extends Encoding {
    /* Record-level Bloom filter encoding was proposed and used by:
  - E.A. Durham, M. Kantarcioglu, Y. Xue, C. Toth, M. Kuzu and B. Malin,
    Composite Bloom filters for secure record linkage. IEEE TKDE 26(12),
    p. 2956-2968, 2014.*/
    private final int random_seed = 42;
    public List<Integer> attr_bf_len_list;
    public List<Integer> attr_q_list;
    public List<Integer> attr_num_hash_functions; // TODO different num of hash functions for each attr
    public List<Boolean> attr_padded_list;
    public List<Boolean> attr_deal_dup_list;
    private List<Integer> perm_index_list;

    RecordBFEncoding(String type, int q, boolean padded, Hashing hash_method, boolean deal_dup) {
        super(type, hash_method, q, padded, deal_dup);
    }
    /* TODO
    Also note that Durham et al. proposed to select bit positions into
    record level Bloom filters based on the number of Bloom filters that
    have 1-bits in a certain given position. We currently do not implement
    this but rather select bit positions randomly. We leave this improvement
    as future work (as it would require to first generate all Bloom filters
    for a database, then to analyse the number of 1-bits per bit position,
    and then to select bit positions accordingly).*/

    @Override
    public void set_attr_param_list(int q, boolean padded, int bf_len, boolean deal_dup) {
        attr_q_list.add(q);
        attr_padded_list.add(padded);
        attr_bf_len_list.add(bf_len);
        attr_deal_dup_list.add(deal_dup);
    }

    private void set_perm_index_lis(int bf_len){
        perm_index_list = new ArrayList<>();
        for (int i = 0; i < bf_len; i++) {
            perm_index_list.add(i);
        }
        Random rnd = new Random();
        rnd.setSeed(random_seed);
        Collections.shuffle(perm_index_list, rnd);
    }

    @Override
    public BitSet rbf_encode(List<String> rec_attr_val_list, int bf_len) {
        BitSet rbf_bf = new BitSet(bf_len);
        //Initial the permutation index list
        if (perm_index_list == null || perm_index_list.size() != bf_len) set_perm_index_lis(bf_len);

        int q;
        boolean padded;
        boolean deal_dup;
        List<String> q_gram_list;
        BitSet attr_bf;
        int cnt = 0;
        for (int i = 0; i < rec_attr_val_list.size(); i++) {
            q = attr_q_list.get(i);
            padded = attr_padded_list.get(i);
            hash_method.bf_len = attr_bf_len_list.get(i);
            deal_dup = attr_deal_dup_list.get(i);

            q_gram_list = qGramListForAttr(rec_attr_val_list.get(i), q, padded);

            if (deal_dup) add_count_2_q_gram(q_gram_list);

            attr_bf = hash_method.hash_q_gram_list(q_gram_list);

            for (int j = cnt; j < attr_bf_len_list.get(i); j++, cnt++) {
                if (attr_bf.get(j)) rbf_bf.set(cnt);
            }
        }

        return rbf_bf;
    }

    @Override
    public BitSet rbf_encode(List<String> rec_attr_val_list, String salt, int bf_len) {
        BitSet rbf_bf = new BitSet(bf_len);
        //Initial the permutation index list
        if (perm_index_list == null || perm_index_list.size() != bf_len) set_perm_index_lis(bf_len);

        int q;
        boolean padded;
        boolean deal_dup;
        List<String> q_gram_list;
        BitSet attr_bf;
        int cnt = 0;
        for (int i = 0; i < rec_attr_val_list.size(); i++) {
            q = attr_q_list.get(i);
            padded = attr_padded_list.get(i);
            hash_method.bf_len = attr_bf_len_list.get(i);
            deal_dup = attr_deal_dup_list.get(i);

            q_gram_list = qGramListForAttr(rec_attr_val_list.get(i), q, padded);

            for (String qGram : q_gram_list) {
                qGram = qGram + salt;
            }

            if (deal_dup) add_count_2_q_gram(q_gram_list);

            attr_bf = hash_method.hash_q_gram_list(q_gram_list);

            for (int j = cnt; j < attr_bf_len_list.get(i); j++, cnt++) {
                if (attr_bf.get(j)) rbf_bf.set(cnt);
            }
        }

        return rbf_bf;
    }

}
