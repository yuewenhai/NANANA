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
        List<List<Integer>> bitVectors = new ArrayList<>();
        encode(bitVectorSize, method, dataset, low, high, bitVectors);

        //多线程计算距离
        List<Double[]> distances = new ArrayList<>();
        util.calDistanceThreads(distances, datasetSize, dataset, bitVectors, low, high, method);

        List<Double> result;
        result = util.calPrecisionRecallF1(distances, threshold);
        return result;
    }

    private void encode(int bitVectorSize, String method, List<Double> dataset, double low, double high, List<List<Integer>> bitVectors) {
        List<Integer> tempBitVector;
        List<Double> randomVector = util.generateRandomVector(bitVectorSize, low, high);
        if (method.equals("PRODPRL_V2"))
            util.generatePseudorandomIndexes(bitVectorSize);
        for (double value : dataset) {
            // 在 util.generateBitVector 中区分使用哪种方法
            tempBitVector = util.generateBitVector(randomVector, value, method);
            bitVectors.add(tempBitVector);
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

        List<Dataset> datasets = new ArrayList<>();
        datasets.add(Dataset.AGES);
        datasets.add(Dataset.HEIGHTS);

        int dataSize = 10000;
        int bitVectorSize = 1000;
        for (Dataset datasetName : datasets) {
            List<Double> dataset = util.generateDataset(datasetName.name(), dataSize, datasetName.low(), datasetName.high());
            util.storeDataset(dataset, datasetName.name());
            for (int t : T) {
                List<List<Double>> result = new ArrayList<>();
                List<List<Double>> result3 = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    System.out.printf("%s T:%d 第%d次%n", datasetName.name(), t, i);
                    result.add(dprl.experiment(datasetName.name(), dataset, bitVectorSize, "DPRL", t));
//                    System.out.println(dprl.experiment("ages", dataSize, bitVectorSize, "PRODPRL_V1", t));
//                    result2.add(dprl.experiment(datasetName, dataSize, bitVectorSize, "PRODPRL_V2", t));
                    result3.add(dprl.experiment(datasetName.name(), dataset, bitVectorSize, "PRODPRL_V3", t));
                }
                File file = new File(String.format("result1/%s result T%d.txt", datasetName.name(), t));
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
