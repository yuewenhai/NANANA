package String;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public enum HashFunction {
    SHA1("SHA1"),
    MD5("MD5");
    private final String method;

    HashFunction(String method) {
        this.method = method;
    }

    public String calculate(String gram) {
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance(this.method);
            messageDigest.update(gram.getBytes());
            byte[] bytes = messageDigest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte aByte : bytes) {
                StringBuilder item = new StringBuilder(Integer.toString(aByte & 0xff, 2));
                while (item.length() < 8)
                    item.insert(0, "0");
                sb.append(item);
            }
            return sb.substring(sb.length() - 32, sb.length());
        } catch (NoSuchAlgorithmException e) {
            return "0";
        }
//        switch (this.method) {
//            case "SHA1" -> {
//                MessageDigest messageDigest = null;
//                try {
//                    messageDigest = MessageDigest.getInstance("SHA1");
//                    messageDigest.update(gram.getBytes());
//                    byte[] bytes = messageDigest.digest();
//                    StringBuilder sb = new StringBuilder();
//                    for (byte aByte : bytes) {
//                        StringBuilder item = new StringBuilder(Integer.toString(aByte & 0xff, 2));
//                        while (item.length() < 8)
//                            item.insert(0, "0");
//                        sb.append(item.toString());
//                    }
//                    return sb.toString();
//                } catch (NoSuchAlgorithmException e) {
//                    return "0";
//                    e.printStackTrace();
//                }
//            }
//            case "MD5" -> {
//                MessageDigest messageDigest = null;
//                try {
//                    messageDigest = MessageDigest.getInstance("MD5");
//                    messageDigest.update(gram.getBytes());
//                    byte[] bytes = messageDigest.digest();
//                    return 1;
//                } catch (NoSuchAlgorithmException e) {
//                    e.printStackTrace();
//                }
//                return 2;
//            }
//            default -> {return 0;}
//        }
    }
}
