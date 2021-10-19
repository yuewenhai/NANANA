package String;

import java.util.ArrayList;
import java.util.List;

import Number.RandomResponse;

public class BloomFilter {
    public List<Double> experiment(String datasetName, int datasetSize, int bitVectorSize, int numGrams, int k, int numAttribute) {
        DatasetGeneration datasetGeneration = new DatasetGeneration();
        List<List<String>> dataset = datasetGeneration.getDataset(datasetName, datasetSize);

        int[][] bitVectors = new int[datasetSize][bitVectorSize];
        List<String> formatDataset = encode(bitVectorSize, dataset, bitVectors, numGrams, k, numAttribute);

        List<List<Double>> similarities = new ArrayList<>();
        List<List<Double>> esSimilarities = new ArrayList<>();
        calSimilarities(formatDataset, bitVectors, similarities, esSimilarities, numGrams);

        List<Double> result = new ArrayList<>();
        return result;
    }

    public List<String> encode(int bitVectorSize, List<List<String>> dataset, int[][] birVectors, int numGrams, int numHashFunction, int numAttribute) {
        RandomResponse rr = new RandomResponse();
        List<String> formatDataset = new ArrayList<>();
        for (int i = 0; i < dataset.size(); i++) {
            StringBuilder attributes = new StringBuilder();
            for (int j = 0; j < numAttribute; j++) {
                attributes.append(dataset.get(i).get(j));
            }
            formatDataset.add(attributes.toString());
            List<String> ngramsList = ngrams(numGrams, attributes.toString());

            int[] bitVector = new int[bitVectorSize];

            for (String gram : ngramsList) {
                String h1 = HashFunction.SHA1.calculate(gram);
                String h2 = HashFunction.MD5.calculate(gram);
                for (int k = 0; k < numHashFunction; k++) {
                    String sum = h1;
                    for (int p = 0; p < k; p++) {
                        sum = add(sum, h2);
                    }
                    int index = Integer.parseInt(sum.substring(sum.length() - 31), 2) % bitVectorSize;
                    bitVector[index] = 1;
                }
            }

//            for (int j = 0;j < bitVectorSize;j++){
//                if (bitVector[j] == 0 && rr.uRandomResponse()) bitVector[j] = 1;
//            }

            birVectors[i] = bitVector;
        }

        return formatDataset;
    }

    public static String add(String a, String b) {
        StringBuilder sb = new StringBuilder();
        int x = 0;
        int y = 0;
        int pre = 0;//进位
        int sum = 0;//存储进位和另两个位的和

        while (a.length() != b.length()) {//将两个二进制的数位数补齐,在短的前面添0
            if (a.length() > b.length()) {
                b = "0" + b;
            } else {
                a = "0" + a;
            }
        }
        for (int i = a.length() - 1; i >= 0; i--) {
            x = a.charAt(i) - '0';
            y = b.charAt(i) - '0';
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

    public List<String> ngrams(int n, String string) {
        List<String> ngrams = new ArrayList<>();
        StringBuilder fix = new StringBuilder();
        fix.append("_".repeat(Math.max(0, n - 1)));
        String fixString = fix + string + fix;
        for (int i = 0; i < fixString.length() - n + 1; i++) {
            ngrams.add(fixString.substring(i, i + n));
        }

        return ngrams;
    }

    public void calSimilarities(List<String> dataset, int[][] bitVectors, List<List<Double>> similarities, List<List<Double>> esSimilarities, int numGrams) {
        int threadNum = 5;//线程数量
        int datasetSize = dataset.size();
        int numPerThread = Math.floorDiv(datasetSize, threadNum) + 1;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int start = i * numPerThread;
            int end = Math.min(start + numPerThread, datasetSize);
            Thread thread = new Thread(() -> {
                double sim;
                double esSim;
                //只计算从该值往后的数之间的距离(不包括该数)
                for (int j = start; j < end; j++) {
                    List<Double> tempSimList = new ArrayList<>();
                    List<Double> tempEstimateSimList = new ArrayList<>();
                    for (int k = j + 1; k < datasetSize; k++) {
                        sim = calSimilarity(dataset.get(j), dataset.get(k), numGrams);
                        esSim = esSimilarity(bitVectors[j], bitVectors[k]);
                        tempSimList.add(sim);
                        tempEstimateSimList.add(esSim);
                    }
                    synchronized (similarities) {
                        similarities.add(tempSimList);
                    }
                    synchronized (esSimilarities) {
                        esSimilarities.add(tempEstimateSimList);
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public double calSimilarity(String string1, String string2, int n) {
        StringBuilder fix = new StringBuilder();
        fix.append("_".repeat(Math.max(0, n - 1)));
        String fixString1 = fix + string1 + fix;
        String fixString2 = fix + string2 + fix;
        int identical = 0;
        int count1 = fixString1.length() - n + 1;
        int count2 = fixString2.length() - n + 1;
        for (int i = 0; i < Math.min(count1, count2); i++) {
            if (fixString1.substring(i, i + n).equals(fixString2.substring(i, i + n))) identical++;
        }

        return (double) identical / (count1 + count2);
    }

    public double esSimilarity(int[] bitVector1, int[] bitVector2) {
        int identical = 0;
        int count1 = 0;
        int count2 = 0;
        for (int i = 0; i < Math.min(bitVector1.length, bitVector2.length); i++) {
            if (bitVector1[i] == 1) count1++;
            if (bitVector2[i] == 1) count2++;
            if (bitVector1[i] == 1 && bitVector2[i] == 1) identical++;
        }

        return (double) 2 * identical / (count1 + count2);
    }

    public static void main(String[] args) {
        BloomFilter bloomFilter = new BloomFilter();
        bloomFilter.experiment("cis.csv", 100, 100, 2, 2, 1);
    }
}
