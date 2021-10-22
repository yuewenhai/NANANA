package String;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import Number.RandomResponse;

public class BloomFilter {
    public void experiment(String datasetName, String[] filenames, int datasetSize, int bitVectorSize,
                           int nGrams, int k, String[] attributes, String method, String key) {
        DatasetGeneration datasetGeneration = new DatasetGeneration();
        List<List<List<String>>> datasets = new ArrayList<>();
        for (String filename : filenames) {
            datasets.add(datasetGeneration.getDataset(datasetName, filename, datasetSize, attributes));
        }

        int[][][] bitVectors = new int[filenames.length][datasetSize][bitVectorSize];
        encode(bitVectorSize, datasets, bitVectors, nGrams, k, method, key);

        List<Double[]> similarities = new ArrayList<>();
        calSimilarities(datasets.get(0), datasets.get(1), bitVectors[0], bitVectors[1], similarities, nGrams);

        storeResult(datasetName, filenames, datasetSize, bitVectorSize, nGrams, k, attributes, method, similarities);
    }

    public void encode(int bitVectorSize, List<List<List<String>>> datasets, int[][][] birVectors, int nGrams,
                       int numHashFunction, String method, String key) {
        RandomResponse rr = new RandomResponse();
        BigInteger bitVectorSizeBig = new BigInteger(String.valueOf(bitVectorSize));
        for (int i = 0; i < datasets.size(); i++) {
            List<List<String>> iDataset = datasets.get(i);
            int iDatasetSize = iDataset.size();
            int[][] iBitVectors = new int[datasets.get(i).size()][];

            int threadNum = 8;//线程数量
            int numPerThread = Math.floorDiv(iDatasetSize, threadNum) + 1;
            List<Thread> threads = new ArrayList<>();
            for (int j = 0; j < threadNum; j++) {
                int start = j * numPerThread;
                int end = Math.min(start + numPerThread, iDatasetSize);
                Thread thread = new Thread(() -> {
                    for (int k = start; k < end; k++) {
                        List<String> ngramsList = ngrams(nGrams, iDataset.get(k));
                        int[] bitVector = new int[bitVectorSize];
                        for (String gram : ngramsList) {
                            BigInteger h1 = HashFunction.SHA1.calculate(gram, key);
                            BigInteger h2 = HashFunction.MD5.calculate(gram, key);
                            for (int q = 0; q < numHashFunction; q++) {
                                BigInteger sum = h1.add(h2.multiply(new BigInteger(String.valueOf(q))));
                                try {
                                    BigInteger index = sum.mod(bitVectorSizeBig);
                                    bitVector[Integer.parseInt(index.toString(10))] = 1;
                                } catch (ArithmeticException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        if (method.equals("ULDP")) {
                            for (int q = 0; q < bitVectorSize; q++) {
                                if (bitVector[q] == 0 && rr.uRandomResponse()) bitVector[q] = 1;
                            }
                        }
                        iBitVectors[k] = bitVector;
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
            birVectors[i] = iBitVectors;
        }
    }

    public List<String> ngrams(int n, List<String> record) {
        List<String> ngrams = new ArrayList<>();
        StringBuilder fix = new StringBuilder();
        fix.append("_".repeat(Math.max(0, n - 1)));
        String fixString = fix + list2String(record) + fix;
        for (int i = 0; i < fixString.length() - n + 1; i++) {
            ngrams.add(fixString.substring(i, i + n));
        }

        return ngrams;
    }

    public void calSimilarities(List<List<String>> dataset1, List<List<String>> dataset2, int[][] bitVectors1,
                                int[][] bitVectors2, List<Double[]> similarities, int numGrams) {
        int threadNum = 8;//线程数量
        int dataset1Size = dataset1.size();
        int dataset2Size = dataset2.size();
        int numPerThread = Math.floorDiv(dataset1Size, threadNum) + 1;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int start = i * numPerThread;
            int end = Math.min(start + numPerThread, dataset1Size);
            Thread thread = new Thread(() -> {
                double sim;
                double esSim;
                //只计算从该值往后的数之间的距离(不包括该数)
                for (int j = start; j < end; j++) {
                    for (int k = 0; k < dataset2Size; k++) {
                        sim = calSimilarity(dataset1.get(j), dataset2.get(k), numGrams);
                        esSim = esSimilarity(bitVectors1[j], bitVectors2[k]);
                        Double[] simTuple = new Double[2];
                        simTuple[0] = sim;
                        simTuple[1] = esSim;
                        synchronized (similarities) {
                            similarities.add(simTuple);
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

    public double calSimilarity(List<String> record1, List<String> record2, int n) {
        int identical = 0;
        List<String> ngrams1 = ngrams(n, record1);
        List<String> ngrams2 = ngrams(n, record2);

        HashSet<String> hashSet = new HashSet<>(ngrams1);

        for (String ngram : ngrams2) {
            if (hashSet.contains(ngram)) identical++;
        }

        return (double) 2 * identical / (ngrams1.size() + ngrams2.size());
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

        if ((count1 + count2) == 0) {
            return 0;
        }
        return (double) 2 * identical / (count1 + count2);
    }

    public void storeResult(String datasetName, String[] filenames, int datasetSize, int bitVectorSize,
                            int nGrams, int k, String[] attributes, String method,
                            List<Double[]> similarities) {
        StringBuilder resultFile = new StringBuilder(datasetName);
        for (String filename : filenames) {
            resultFile.append("-").append(filename, 0, filename.length() - 4);
        }
        resultFile.append("-").append(datasetSize).append("-").append(bitVectorSize).append("-").append(nGrams).append("-").append(k);
        for (String attribute : attributes) {
            resultFile.append("-").append(attribute);
        }
        String filepath = "StringResult/" + method;
        File fileDir = new File(filepath);
        if (!fileDir.exists()) {
            if (!fileDir.mkdirs()) {
                System.out.println("创建文件夹失败");
                return;
            }
        }
        int threadNum = 8;
        int numPerThread = Math.floorDiv(similarities.size(), threadNum) + 1;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int start = i * numPerThread;
            int end = Math.min(start + numPerThread, similarities.size());
            File file = new File(filepath + "/" + resultFile + "-" + i + ".txt");
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Thread thread = new Thread(() -> {
                try (FileWriter fw = new FileWriter(file, false);
                     BufferedWriter bw = new BufferedWriter(fw)) {
                    for (int j = start; j < end; j++) {
                        String result = similarities.get(j)[0] + " " + similarities.get(j)[1];
                        bw.write(result + "\n");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
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

    public String list2String(List<String> list) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String str : list) {
            stringBuilder.append(str);
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        BloomFilter bloomFilter = new BloomFilter();
        String[] filenames = {"census.csv", "cis.csv"};
        String[][] attributes = {{"PERNAME2"},
                {"PERNAME1", "PERNAME2"},
                {"PERNAME1", "PERNAME2", "SEX"},
                {"PERNAME1", "PERNAME2", "SEX", "DOB_DAY", "DOB_MON", "DOB_TEAR"},
                {"PERSON_ID", "PERNAME1", "PERNAME2", "SEX", "DOB_DAY", "DOB_MON", "DOB_TEAR"}};
        String key = "";
        for (String[] tempAttributes : attributes) {
            bloomFilter.experiment("Istat", filenames, 10000, 1000, 2, 30, tempAttributes, "Normal", key);
            bloomFilter.experiment("Istat", filenames, 10000, 1000, 2, 30, tempAttributes, "ULDP", key);
        }
    }
}
