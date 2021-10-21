package Number;

import java.util.ArrayList;
import java.util.List;

public class UEULDP {
    private final Util util = new Util();

    public List<Double> experiment(String datasetName, int datasetSize, int threshold){
        //生成数据集
        List<Double> dataset = new ArrayList<>();
        double low = 0.0;//数据集的下界
        double high = 0.0;//数据集的上界
        if (datasetName.equals("heights")) {
            low = Dataset.HEIGHTS.low();
            high = Dataset.HEIGHTS.high();
            dataset = util.generateDataset(datasetName, datasetSize, low, high);
        } else if (datasetName.equals("ages")) {
            low = Dataset.AGES.low();
            high = Dataset.AGES.high();
            dataset = util.generateDataset(datasetName, datasetSize, low, high);
        }

        //为每一个数字生成bit vector
        int[][] bitVectors = new int[datasetSize][(int) ((high - low) / util.gap + 1)];
        encode(dataset, low, high, bitVectors);

        //多线程计算距离
        List<Double[]> dis = new ArrayList<>();
        util.calDistanceThreads(dis, datasetSize, dataset, bitVectors, low, high, "DPRL.UEULDP");

        List<Double> result;
        result = util.calPrecisionRecallF1(dis, threshold);
        return result;
    }

    private void encode(List<Double> dataset, double low, double high, int[][] bitVectors) {
        int[] tempBitVector;
        for (int i = 0; i < dataset.size();i++) {
            // 在 util.generateBitVector 中区分使用哪种方法
            tempBitVector = util.generateBitVector(dataset.get(i), low, high);
            bitVectors[i] = tempBitVector;
        }
    }

    public static void main(String[] args) {
        UEULDP ueuldp = new UEULDP();
        List<Integer> T = new ArrayList<>();
        T.add(3);
        T.add(5);
        T.add(7);

        int dataSize = 1000;
        for (int t : T) {
//            System.out.println(dprl.encode("ages", dataSize, bitVectorSize, "DPRL.DPRL", t));
//            System.out.println(dprl.encode("ages", dataSize, bitVectorSize, "PRODPRL_V1", t));
//            System.out.println(dprl.encode("ages", dataSize, bitVectorSize, "PRODPRL_V2", t));
            System.out.println(ueuldp.experiment("ages", dataSize, t));
        }
    }
}
