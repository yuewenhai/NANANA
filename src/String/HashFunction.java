package String;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public enum HashFunction {
    SHA1("SHA1", "HmacSHA1"),
    MD5("MD5", "HmacMD5");
    private final String method;
    private final String hmacMethod;

    HashFunction(String method, String hmacMethod) {
        this.method = method;
        this.hmacMethod = hmacMethod;
    }

    public BigInteger calculate(String gram, String key) {
        try {
            if (key.equals("")){
                MessageDigest messageDigest = MessageDigest.getInstance(this.method);
                messageDigest.update(gram.getBytes());
                byte[] bytes = messageDigest.digest();
                return new BigInteger(1, bytes);
            }else {
                SecretKey secretKey = new SecretKeySpec(key.getBytes(), this.hmacMethod);
                Mac mac = Mac.getInstance(this.hmacMethod);
                mac.init(secretKey);
                mac.update(gram.getBytes());
                byte[] bytes = mac.doFinal();
                return new BigInteger(1, bytes);
            }
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            return new BigInteger("0");
        }
    }
}
