package Number;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Util {
    private final int n = 10;
    public final double gap = 1.0;
    private final List<Integer> pseudorandomIndexes = new ArrayList<>();

    /**
     * 生成数据集
     *
     * @param datasetName 数据集名字
     * @param size        数据集大小
     * @param low         数据集最小值
     * @param high        数据集最大值
     * @return 返回数据集
     */
    public List<Double> generateDataset(String datasetName, int size, double low, double high) {
        List<Double> dataset = new ArrayList<>();
        Random random = new Random();
        double value;
        double randomValue;
        switch (datasetName) {
            case "HEIGHTS" -> {
                double mean = (low + high) / 2;
                double half_half = (mean - low) / 2;
                for (int i = 0; i < size; i++) {
                    randomValue = random.nextGaussian();
                    value = randomValue * half_half + mean;
                    if (value < low)
                        dataset.add(low);
                    else dataset.add(Math.floor(Math.min(value, high)));
                }
            }
            case "AGES" -> {
                for (int i = 0; i < size; i++) {
                    randomValue = random.nextDouble();
                    value = Math.floor(randomValue * (high - low) + low);
                    dataset.add(value);
                }
            }
        }

        return dataset;
    }

    public void storeDataset(List<Double> dataset, String datasetName){
        File file = new File(String.format("dataset/%s.txt", datasetName));
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try (FileWriter fw = new FileWriter(file, false);
             BufferedWriter bw = new BufferedWriter(fw)) {
            for (int i = 0; i < dataset.size(); i++) {
                bw.write(dataset.get(i) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 生成随机数组
     *
     * @param size bit vector位数
     * @param low  随机数最小值
     * @param high 随机数最大值
     * @return 返回random vector
     */
    public List<Double> generateRandomVector(int size, double low, double high) {
        List<Double> randomVector = new ArrayList<>();
        Random random = new Random();
        double randomValue;
        double tempDouble;
        for (int i = 0; i < size; i++) {
            tempDouble = random.nextDouble();
            randomValue = (high - low) * tempDouble + low;
            randomVector.add(randomValue);
        }
        return randomVector;
    }

    public void generatePseudorandomIndexes(int size) {
        Random rand = new Random();
        int segmentNum = size / n;
        if (size % n != 0)
            segmentNum += 1;
        for (int i = 0; i < segmentNum; i++) {
            int start = i * n;
            int end = Math.min((i + 1) * n, size);
            int gap = end - start;
            int index = start + rand.nextInt(gap);//随机选择一位
            pseudorandomIndexes.add(index);
        }
    }

    /**
     * DPRL.DPRL 对值进行使用特定方法编码
     *
     * @param randomVector 随机数组
     * @param value        被编码的值
     * @param method       使用方法
     * @return 返回bit vector
     */
    public int[] generateBitVector(List<Double> randomVector, double value, String method) {
        RandomResponse rr = new RandomResponse();
        int size = randomVector.size();
        int[] bitVector = new int[size];
        for (int i = 0;i < size;i++) {
            if (value >= randomVector.get(i)) bitVector[i] = 1;
            else bitVector[i] = 0;
        }
        // DPRL.DPRL,使用coin flipping
        if (method.equals("DPRL")) {
            for (int i = 0; i < size; i++) {
                if (rr.coinFlipping()) bitVector[i] = 1 - bitVector[i];
            }
        }
        // PRODPRL_V1,随机选择一位使用random response
        if (method.equals("PRODPRL_V1")) {
            Random rand = new Random();
            int segmentNum = size / n;
            if (size % n != 0)
                segmentNum += 1;
            for (int i = 0; i < segmentNum; i++) {
                int start = i * n;
                int index = Math.min(start + rand.nextInt(n), size);//随机选择一位
                if (rr.randomResponse()) bitVector[index] = 1 - bitVector[index];
            }
        }
        // PRODPRL_V2,双方根据统一的伪随机序列选择RR的位使用random response
        if (method.equals("PRODPRL_V2")) {
            for (int index : pseudorandomIndexes) {
                if (rr.randomResponse()) bitVector[index] = 1 - bitVector[index];
            }
        }
        // PRODPRL_V3,满足ULDP
        if (method.equals("PRODPRL_V3")) {
            for (int i = 0; i < size; i++) {
                if (bitVector[i] == 0 && rr.uRandomResponse()) bitVector[i] = 1;
            }
        }
        return bitVector;
    }

    /**
     * DPRL.UEULDP 对值进行使用特定方法编码
     *
     * @param value 被编码的值
     * @param low   定义域最小值
     * @param high  定义域最大值
     * @return 编码结果
     */
    public int[] generateBitVector(double value, double low, double high) {
        RandomResponse rr = new RandomResponse();
        int bitVectorSize = (int) ((high - low) / gap + 1);
        int[] bitVector = new int[bitVectorSize];
        boolean setted = false;
        for (int i = 0; i < bitVectorSize; i++) {
            if (value >= low + i * gap || setted) bitVector[i] = 0;
            else {
                bitVector[i] = 1;
                setted = true;
            }
        }

        for (int i = 0; i < bitVectorSize; i++) {
            if (bitVector[i] == 0 && rr.uRandomResponse()) bitVector[i] = 1;
        }
        return bitVector;
    }

    public int calHammingDis(int[] bitVector1, int[] bitVector2) {
        int hammingDis = 0;
        for (int i = 0; i < Math.min(bitVector1.length, bitVector2.length); i++) {
            if (bitVector1[i] != bitVector2[i]) {
                hammingDis += 1;
            }
        }

        return hammingDis;
    }

    /**
     * 该方法为通过bit vector估计距离
     *
     * @param bitVector1 bit vector 1
     * @param bitVector2 bit vector 2
     * @param low        定义域最小值
     * @param high       定义域最大值
     * @param method     所使用的方法
     * @return 返回估计的距离
     */
    public Double estimate(int[] bitVector1, int[] bitVector2, double low, double high, String method) {
        RandomResponse rr = new RandomResponse();
        double u = high - low;
        double hammingDis = calHammingDis(bitVector1, bitVector2) / 1.1;
        int size = bitVector1.length;
        double estimateDis;
        switch (method) {
            case "DPRL" -> estimateDis = (hammingDis / size - (1 - Math.pow(rr.coinFlipProb, 2)) / 2) * u / Math.pow(rr.coinFlipProb, 2);
            case "PRODPRL_V1", "PRODPRL_V2" -> {
                int segmentNum = size / n;
                if (size % n != 0)
                    segmentNum += 1;
                double subtract = 0.0;
                double divide = 0.0;
                double prob = Math.exp(rr.epsilon) / (Math.exp(rr.epsilon) + 1);
                for (int i = 0; i < segmentNum; i++) {
                    int start = i * n;
                    int end = Math.min((i + 1) * n, size);
                    int subSize = end - start;
                    if (method.equals("PRODPRL_V1")) {
                        subtract += (2 * prob / subSize - 2 * Math.pow(prob, 2) / subSize + (2 - 2 * prob) * (
                                subSize - 1) / subSize);
                        divide += ((subSize - 1 + Math.pow((2 * prob - 1), 2)) + (subSize - 1) * (subSize - 4 + 4 * prob)) / subSize;
                    } else {
                        subtract += (2 * prob - 2 * Math.pow(prob, 2));
                        divide += (Math.pow((2 * prob - 1), 2) + subSize - 1);
                    }
                }

                estimateDis = (hammingDis - subtract) * u / divide;
            }
            case "PRODPRL_V3" -> {
                double estimateValue1 = estimateValue(bitVector1, high, low, rr.epsilon);
                double estimateValue2 = estimateValue(bitVector2, high, low, rr.epsilon);
                double prob = 1 / Math.exp(rr.epsilon);
                estimateDis = hammingDis * u / size / (1 - prob) - ((high - Math.max(estimateValue1, estimateValue2)) * 2 * prob);
                //estimateDis = Math.abs(estimateValue1 - estimateValue2);
            }
            case "DPRL.UEULDP" -> {
                int bitVectorSize = bitVector1.length;
                double sum1 = 0.0;
                double sum2 = 0.0;
                for (int i = 0;i < bitVectorSize;i++){
                    if (bitVector1[i] == 1) sum1 += (low + i * gap);
                    if (bitVector2[i] == 1) sum2 += (low + i * gap);
                }
                double prob = 1 / Math.exp(rr.epsilon);
                estimateDis = Math.abs(sum1 - sum2) / (1 - prob);
            }
            default -> estimateDis = hammingDis * u / size;
        }

        return estimateDis;
    }

    private double estimateValue(int[] bitVector, double high, double low, double epsilon) {
        double prob = 1 / Math.exp(epsilon);
        double u = high - low;
        int size = bitVector.length;
        int hammingWeight = 0;
        for (int bit : bitVector) {
            if (bit == 1) {
                hammingWeight += 1;
            }
        }

        return ((double) hammingWeight / size * u - high * prob + low) / (1 - prob);
    }

    /**
     * @param distances   真实距离和估计距离, {[真实距离,估计距离]}
     * @param datasetSize 数据集大小
     * @param dataset     数据集
     * @param bitVectors  编码的bit vectors
     * @param low         数据集最小值
     * @param high        数据集最大值
     * @param method      使用方法
     */
    public void calDistanceThreads(List<Double[]> distances, int datasetSize,
                                   List<Double> dataset, int[][] bitVectors,
                                   double low, double high, String method) {
        int threadNum = 8;//线程数量

        int numPerThread = Math.floorDiv(datasetSize, threadNum) + 1;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int start = i * numPerThread;
            int end = Math.min(start + numPerThread, datasetSize);
            Thread thread = new Thread(() -> {
                double dis;
                double esDis;
                //只计算从该值往后的数之间的距离(不包括该数)
                for (int j = start; j < end; j++) {
                    Double[] tempDistances = new Double[2];
                    for (int k = j + 1; k < datasetSize; k++) {
                        dis = Math.abs(dataset.get(j) - dataset.get(k));
                        esDis = estimate(bitVectors[j], bitVectors[k], low, high, method);
                        tempDistances[0] = dis;
                        tempDistances[1] = esDis;
                        synchronized (distances) {
                            distances.add(tempDistances);
                        }
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

    /**
     * 计算准确率、召回率、F1
     * Precision = TP / (TP + FP);
     * Recall = TP / (TP + FN);
     * F1 = 2 * Precision * Recall / (Precision + Recall);
     *
     * @param disAndEsDis       真实距离矩阵\
     * @param threshold 距离阈值
     * @return {precision, recall, F1}
     */
    public List<Double> calPrecisionRecallF1(List<Double[]> disAndEsDis, int threshold) {
        List<Double> result = new ArrayList<>();
        int TP = 0;
        int FP = 0;
        int TN = 0;
        int FN = 0;
        for (Double[] disAndEsDisArray : disAndEsDis) {
            double tempEuDis = disAndEsDisArray[0];
            double tempEsDis = disAndEsDisArray[1];
            if (tempEuDis <= threshold) {
                if (tempEsDis <= threshold)
                    TP += 1;
                else
                    FN += 1;
            } else {
                if (tempEsDis <= threshold)
                    FP += 1;
                else
                    TN += 1;
            }
        }
        double precision = (double) TP / (TP + FP);
        double recall = (double) TP / (TP + FN);
        double f1 = 2 * precision * recall / (precision + recall);

        result.add(precision);
        result.add(recall);
        result.add(f1);
        return result;
    }
}
