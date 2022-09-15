package String;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Hashing {
    public String type;
    public int bf_len;
    public int num_hash_function;

    Hashing(String type, int bf_len, int num_hash_function){
        this.type = type;
        this.bf_len = bf_len;
        this.num_hash_function = num_hash_function;
    }

    public BitSet hash_q_gram_list(List<String> q_gram_list){
        return null;
    }

    public BigInteger hashFunction(String hash_function, String q_gram){
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance(hash_function);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        assert messageDigest != null;
        messageDigest.update(q_gram.getBytes());
        byte[] bytes = messageDigest.digest();
        return new BigInteger(1, bytes);
    }

    public BigInteger hashFunctionKeyed(String hash_function, String key, String q_gram){
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance(hash_function);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        assert messageDigest != null;
        messageDigest.update(key.getBytes());
        messageDigest.update(q_gram.getBytes());
        byte[] bytes = messageDigest.digest();
        return new BigInteger(1, bytes);
    }
}

class DoubleHashing extends Hashing {
    public String hash_function1;
    public String hash_function2;

    DoubleHashing(String type, String hash_function1_, String hash_function2_, int bf_len_, int num_hash_functions_) {
        super(type, bf_len_, num_hash_functions_);
        this.hash_function1 = hash_function1_;
        this.hash_function2 = hash_function2_;
    }

    @Override
    public BitSet hash_q_gram_list(List<String> q_gram_list) {
        int k = this.num_hash_function;
        BitSet bf = new BitSet(this.bf_len);

        Set<String> q_gram_set = new HashSet<>(q_gram_list);
        for (String q_gram : q_gram_set) {
            BigInteger hash_value1 = hashFunction(this.hash_function1, q_gram);
            BigInteger hash_value2 = hashFunction(this.hash_function2, q_gram);

            BigInteger sum = hash_value1;
            int index;
            for (int i = 1; i <= k; i++) {
                sum = sum.add(hash_value2);
                index = (sum.intValue() & 0x7FFFFFFF) % this.bf_len;
                bf.set(index);
            }
        }
        return bf;
    }
}

class EnhancedDoubleHashing extends DoubleHashing {
    public String hash_function1;
    public String hash_function2;

    EnhancedDoubleHashing(String type, String hash_function1_, String hash_function2_, int bf_len_, int num_hash_functions_) {
        super(type, hash_function1_, hash_function2_, bf_len_, num_hash_functions_);
    }

    @Override
    public BitSet hash_q_gram_list(List<String> q_gram_list) {
        int k = this.num_hash_function;
        BitSet bf = new BitSet(this.bf_len);

        Set<String> q_gram_set = new HashSet<>(q_gram_list);
        for (String q_gram : q_gram_set) {
            BigInteger hash_value1 = hashFunction(this.hash_function1, q_gram);
            BigInteger hash_value2 = hashFunction(this.hash_function2, q_gram);

            BigInteger sum = hash_value1.add(hash_value2);
            int sumInt = sum.intValue() & 0x7FFFFFFF;
            int index;
            for (int i = 1; i <= k; i++) {
                sumInt += (Math.pow(i, 3) - i) / 6;
                index = sumInt % this.bf_len;
                bf.set(index);
            }
        }
        return bf;
    }
}


class RandomHashing extends Hashing {
    public String hash_function;

    RandomHashing(String type, String hash_function, int bf_len_, int num_hash_functions_) {
        super(type, bf_len_, num_hash_functions_);
        this.hash_function = hash_function;
    }

    @Override
    public BitSet hash_q_gram_list(List<String> q_gram_list) {
        int k = this.num_hash_function;
        BitSet bf = new BitSet(this.bf_len);

        Set<String> q_gram_set = new HashSet<>(q_gram_list);
        for (String q_gram : q_gram_set) {
            BigInteger hash_value = hashFunction(this.hash_function, q_gram);

            Random random = new Random(hash_value.intValue() & 0x7FFFFFFF);
            int index_i;
            for (int i = 1; i <= k; i++) {
                index_i = random.nextInt(this.bf_len);
                bf.set(index_i);
            }
        }
        return bf;
    }
}

