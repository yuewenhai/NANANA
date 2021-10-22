package String;

public class BinaryOperation {

    public String binaryMultiply(String h2, int q) {
        if (q == 0)
            return "0";
        else if (q == 1) return h2;
        else {
            String qBinary = Integer.toBinaryString(q);
            String sum = "0";
            for (int i = qBinary.length() - 1;i >= 0;i--){
                if (qBinary.charAt(i) == '1') {
                    sum = binaryAdd(sum, h2 + "0".repeat(Math.max(0, qBinary.length() - 1 - i)));
                }
            }
            return sum;
        }
    }

    public String binaryAdd(String binary1, String binary2) {
        if (binary1.equals("0")) return binary2;
        if (binary2.equals("0")) return binary1;
        StringBuilder sb = new StringBuilder();
        int x = 0;
        int y = 0;
        int pre = 0;//进位
        int sum = 0;//存储进位和另两个位的和

        while (binary1.length() != binary2.length()) {//将两个二进制的数位数补齐,在短的前面添0
            if (binary1.length() > binary2.length()) {
                binary2 = "0" + binary2;
            } else {
                binary1 = "0" + binary1;
            }
        }
        for (int i = binary1.length() - 1; i >= 0; i--) {
            x = binary1.charAt(i) - '0';
            y = binary2.charAt(i) - '0';
            sum = x + y + pre;//从低位做加法
            if (sum >= 2) {
                pre = 1;//进位
                sb.append(sum - 2);
            } else {
                pre = 0;
                sb.append(sum);
            }
        }
        if (pre == 1) {
            sb.append("1");
        }
        return sb.reverse().toString();//翻转返回
    }

    public String xor(String binary1, String binary2){
        StringBuilder result = new StringBuilder();
        for (int i = 0;i < binary1.length();i++){
            if (binary1.charAt(i) == binary2.charAt(i)) result.append(0);
            else result.append(1);
        }
        return result.toString();
    }

    public String binaryMod(String binary, int divide) throws ArithmeticException{
        if (divide == 0) {
            throw new ArithmeticException("二进制取余除数为0");
        }
        else if (divide == 1) return "0";
        else {
            int begin = 0;
            for (int i = 0;i < binary.length();i++){
                if (binary.charAt(i) == '0') begin++;
                else break;
            }
            binary = binary.substring(begin);
            String divideBinary = Integer.toBinaryString(divide);
            String pre = Integer.toBinaryString(Integer.parseInt(xor(binary.substring(0, divideBinary.length()), divideBinary), 2));
            for (int i = divideBinary.length();i < binary.length();i++){
                pre = Integer.toBinaryString(Integer.parseInt(pre + binary.charAt(i), 2));
                if (pre.length() >= divideBinary.length()) {
                    pre = Integer.toBinaryString(Integer.parseInt(xor(pre, divideBinary), 2));
                }
            }
            if (pre.length() == divideBinary.length()) pre = xor(pre, divideBinary);
            return pre;
        }
    }
}
