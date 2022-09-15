package Number;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DPRL {
    private final Util util = new Util();

    public List<Double> experiment(String datasetName, List<Double> dataset, int bitVectorSize, String method, int threshold) {
        //生成数据集
        double low = 0.0;//数据集的下界
        double high = 0.0;//数据集的上界
        if (datasetName.equals("HEIGHTS")) {
            low = Dataset.HEIGHTS.low();
            high = Dataset.HEIGHTS.high();
        } else if (datasetName.equals("AGES")) {
            low = Dataset.AGES.low();
            high = Dataset.AGES.high();
        }
        int datasetSize = dataset.size();

        //为每一个数字生成bit vector
        int[][] bitVectors = new int[datasetSize][bitVectorSize];
        encode(bitVectorSize, method, dataset, low, high, bitVectors);

        //多线程计算距离
        List<Double[]> distances = new ArrayList<>();
        util.calDistanceThreads(distances, datasetSize, dataset, bitVectors, low, high, method);

        List<Double> result;
        result = util.calPrecisionRecallF1(distances, threshold);
        return result;
    }

    private void encode(int bitVectorSize, String method, List<Double> dataset, double low, double high, int[][] bitVectors) {
        List<Double> randomVector = util.generateRandomVector(bitVectorSize, low, high);
        if (method.equals("PRODPRL_V2"))
            util.generatePseudorandomIndexes(bitVectorSize);

        int threadNum = 8;//线程数量
        int datasetSize = dataset.size();
        int numPerThread = Math.floorDiv(datasetSize, threadNum) + 1;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int start = i * numPerThread;
            int end = Math.min(start + numPerThread, datasetSize);
            Thread thread = new Thread(() -> {
                for (int j = start;j < end;j++) {
                    // 在 util.generateBitVector 中区分使用哪种方法
                    int[] tempBitVector;
                    tempBitVector = util.generateBitVector(randomVector, dataset.get(j), method);
                    bitVectors[j] = tempBitVector;
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

    public static void main(String[] args) {
        DPRL dprl = new DPRL();
        Util util = new Util();
        List<Integer> T = new ArrayList<>();
        T.add(3);
        T.add(5);
        T.add(7);
        T.add(9);

        List<Double> epsilons = new ArrayList<>();
        epsilons.add(0.0);
        epsilons.add(0.5);
        epsilons.add(1.0);
        epsilons.add(1.5);
        epsilons.add(2.0);
        epsilons.add(2.5);
        epsilons.add(3.0);

        List<Double> hts = new ArrayList<>();
        hts.add(1.0);
        hts.add(1.1);
        hts.add(1.2);
        hts.add(1.3);

        List<Dataset> datasets = new ArrayList<>();
        datasets.add(Dataset.AGES);
        datasets.add(Dataset.HEIGHTS);

        RandomResponse rr = new RandomResponse();

        int dataSize = 2000;
        int bitVectorSize = 1000;
        for (Dataset datasetName : datasets) {
            List<Double> dataset = util.generateDataset(datasetName.name(), dataSize, datasetName.low(), datasetName.high());
            util.storeDataset(dataset, datasetName.name());
            for (double ht : hts) {
                util.setHt(ht);
                for (double epsilon : epsilons) {
                    rr.setEpsilon(epsilon);
                    for (int t : T) {
                        List<List<Double>> result = new ArrayList<>();
                        List<List<Double>> result3 = new ArrayList<>();
                        for (int i = 0; i < 20; i++) {
                            System.out.printf("%s ht:%.2f T:%d ep:%.2f 第%d次%n", datasetName.name(), util.ht, t, rr.epsilon, i);
                            result.add(dprl.experiment(datasetName.name(), dataset, bitVectorSize, "DPRL", t));
//                    System.out.println(dprl.experiment("ages", dataSize, bitVectorSize, "PRODPRL_V1", t));
//                    result2.add(dprl.experiment(datasetName, dataSize, bitVectorSize, "PRODPRL_V2", t));
                            result3.add(dprl.experiment(datasetName.name(), dataset, bitVectorSize, "PRODPRL_V3", t));
                        }
                        File file = new File(String.format("result1.2/%s ht-%.2f result T%d ep%.2f.txt", datasetName.name(), util.ht, t, rr.epsilon));
                        if (!file.exists()) {
                            try {
                                file.createNewFile();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try (FileWriter fw = new FileWriter(file, false);
                             BufferedWriter bw = new BufferedWriter(fw)) {
                            for (int i = 0; i < result.size(); i++) {
                                StringBuilder resultTemp = new StringBuilder();
                                StringBuilder result3Temp = new StringBuilder();
                                for (int j = 0; j < result.get(i).size(); j++) {
                                    resultTemp.append(result.get(i).get(j).toString()).append(" ");
                                    result3Temp.append(result3.get(i).get(j).toString()).append(" ");
                                }
                                bw.write(resultTemp.append("\n").toString());
                                bw.write(result3Temp.append("\n").toString());
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
