package String;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

import Number.RandomResponse;

public class BloomFilter {
    public final int wSize = 1;
    public final int randomSeed = 42;
    public final int blockIndex = 1;
    public final int blockKeyLength = 16;
    public final int blockKeyNum = 30;

    public void experiment(String datasetName, String[] filenames, int datasetSize, int bitVectorSize,
                           int nGrams, int k, String[] attributes, String method, String key, String blockingMethod) {
        System.out.println(method + "----------------------------");
        DatasetGeneration datasetGeneration = new DatasetGeneration();
        List<List<List<String>>> datasets = new ArrayList<>();
        for (String filename : filenames) {
            datasets.add(datasetGeneration.getDataset(datasetName, filename, datasetSize, attributes));
        }

        System.out.println("encoding");
        long startTime = System.currentTimeMillis();
        int[][][] bitVectors = new int[filenames.length][datasetSize][bitVectorSize];
        encode(bitVectorSize, datasets, bitVectors, nGrams, k, method, key);
        long endTime = System.currentTimeMillis();
        System.out.println("encoding 耗时:" + (double)(endTime - startTime) / 1000);

        System.out.println("blocking");
        startTime = System.currentTimeMillis();
        List<Map<String, List<List<String>>>> blockDatasets = new ArrayList<>();
        List<Map<String, List<List<Integer>>>> blockBitVectors = new ArrayList<>();
        blocking(datasets, bitVectors, blockingMethod, blockDatasets, blockBitVectors, bitVectorSize);
        endTime = System.currentTimeMillis();
        System.out.println("blocking 耗时:" + (double)(endTime - startTime) / 1000);

        System.out.println("cal similarities");
        startTime = System.currentTimeMillis();
        List<Double[]> similarities = new ArrayList<>();
        calSimilarities(blockDatasets.get(0), blockDatasets.get(1), blockBitVectors.get(0), blockBitVectors.get(1), similarities, nGrams);
        endTime = System.currentTimeMillis();
        System.out.println("cal similarities 耗时:" + (double)(endTime - startTime) / 1000);

        System.out.println("store results");
        startTime = System.currentTimeMillis();
        storeResult(datasetName, filenames, datasetSize, bitVectorSize, nGrams, k, attributes, method, similarities, blockingMethod);
        endTime = System.currentTimeMillis();
        System.out.println("store results 耗时:" + (double)(endTime - startTime) / 1000);
    }

    private void blocking(List<List<List<String>>> datasets, int[][][] bitVectors, String blockingMethod,
                          List<Map<String, List<List<String>>>> blockDatasets,
                          List<Map<String, List<List<Integer>>>> blockBitVectors, int bitVectorSize) {
        int[][] bigThetas = generateBlockBigThetas(bitVectorSize);

        for (int i = 0;i < datasets.size();i++){
            List<List<String>> ithDataset = datasets.get(i);
            int[][] ithBitVectors = bitVectors[i];

            Map<String, List<List<String>>> ithBlockDataset = new HashMap<>();
            Map<String, List<List<Integer>>> ithBlockBitVectors = new HashMap<>();
            List<Integer> tempList = new ArrayList<>();
            if (blockingMethod.equals("Soundex")) {
                for (int j = 0; j < ithDataset.size(); j++) {
                    List<String> record = ithDataset.get(j);
                    int[] recordBitVector = ithBitVectors[j];
                    String pername2 = record.get(blockIndex);
                    String soundex = generateSoundex(pername2);
                    addRecord(ithBlockDataset, ithBlockBitVectors, tempList, record, recordBitVector, soundex);
                }
            }else if (blockingMethod.equals("HLSH")) {
                for (int j = 0;j < ithBitVectors.length;j++){
                    List<String> record = ithDataset.get(j);
                    int[] recordBitVector = ithBitVectors[j];

                    for (int[] theta : bigThetas) {
                        StringBuilder keyBuilder = new StringBuilder();
                        for (int f : theta) {
                            keyBuilder.append(recordBitVector[f]);
                        }
                        String key = keyBuilder.toString();
                        addRecord(ithBlockDataset, ithBlockBitVectors, tempList, record, recordBitVector, key);
                    }
                }
            }

            blockDatasets.add(ithBlockDataset);
            blockBitVectors.add(ithBlockBitVectors);
        }
    }

    private void addRecord(Map<String, List<List<String>>> ithBlockDataset, Map<String, List<List<Integer>>> ithBlockBitVectors, List<Integer> tempList, List<String> record, int[] recordBitVector, String key) {
        List<List<String>> tempDataset;
        List<List<Integer>> tempBitVector;
        if (!ithBlockDataset.containsKey(key)) {
            tempDataset = new ArrayList<>();
            tempDataset.add(record);
            ithBlockDataset.put(key, tempDataset);
        } else {
            tempDataset = ithBlockDataset.get(key);
            tempDataset.add(record);
        }
        tempList.clear();
        if (!ithBlockBitVectors.containsKey(key)) {
            tempBitVector = new ArrayList<>();
            array2List(recordBitVector, tempList);
            tempBitVector.add(tempList);
            ithBlockBitVectors.put(key, tempBitVector);
        } else {
            array2List(recordBitVector, tempList);
            tempBitVector = ithBlockBitVectors.get(key);
            tempBitVector.add(tempList);
        }
    }

    private int[][] generateBlockBigThetas(int length) {
        Random random = new Random();
        int[][] bigThetas = new int[blockKeyNum][blockKeyLength];
        for (int i = 0;i < blockKeyNum;i++){
            int[] theta = new int[blockKeyLength];
            for (int j = 0;j < blockKeyLength;j++){
                theta[j] = random.nextInt(length);
            }
            bigThetas[i] = theta;
        }
        return bigThetas;
    }

    private void array2List(int[] recordBitVector, List<Integer> tempList) {
        for (int value : recordBitVector){
            tempList.add(value);
        }
    }

    private String generateSoundex(String pername2) {
        List<String> soundex = new ArrayList<>();

        pername2 = pername2.toLowerCase(Locale.ROOT);
        for (int i = 0;i < pername2.length();i++){
            String letter = String.valueOf(pername2.charAt(i));
            if (i == 0){
                soundex.add(letter);
                continue;
            }
            switch (letter){
                case "b", "f", "p", "v" -> {
                    if (!soundex.get(soundex.size() - 1).equals("1")) soundex.add("1");
                }
                case "l" -> {
                    if (!soundex.get(soundex.size() - 1).equals("4")) soundex.add("4");
                }
                case "c", "g", "j", "k", "q", "s", "x", "z" -> {
                    if (!soundex.get(soundex.size() - 1).equals("2")) soundex.add("2");
                }
                case "m" ,"n" -> {
                    if (!soundex.get(soundex.size() - 1).equals("5")) soundex.add("5");
                }
                case "d" ,"t" -> {
                    if (!soundex.get(soundex.size() - 1).equals("3")) soundex.add("3");
                }
                case "r" -> {
                    if (!soundex.get(soundex.size() - 1).equals("6")) soundex.add("6");
                }
            }
        }

        while (soundex.size() < 4){
            soundex.add("0");
        }

        return list2String(soundex.subList(0, 4));
    }

    private String list2String(List<String> soundex) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String item : soundex){
            stringBuilder.append(item);
        }
        return stringBuilder.toString();
    }

    public void encode(int bitVectorSize, List<List<List<String>>> datasets, int[][][] birVectors, int nGrams,
                       int numHashFunction, String method, String key) {
        RandomResponse rr = new RandomResponse();
        BigInteger bitVectorSizeBig = new BigInteger(String.valueOf(bitVectorSize));

        for (int i = 0; i < datasets.size(); i++) {
            List<List<String>> iDataset = datasets.get(i);
            int iDatasetSize = iDataset.size();
            int[][] iBitVectors = new int[datasets.get(i).size()][];

            int threadNum = 5;//线程数量
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
                        switch (method) {
                            case "ULDP" -> {
                                for (int q = 0; q < bitVectorSize; q++) {
                                    if (bitVector[q] == 0 && rr.uRandomResponse()) bitVector[q] = 1;
                                }
                            }
                            case "WXOR" -> {
                                for (int q = 0;q < bitVectorSize;q++){
                                    for (int count = 0;count < wSize;count++){
                                        int position = (q + count) % bitVectorSize;
                                        int next = (q + count + 1) % bitVectorSize;
                                        if (bitVector[position] == bitVector[next]) bitVector[position] = 0;
                                        else bitVector[position] = 1;
                                    }
                                }
                            }
                            case "RESAM" -> {
                                Random random = new Random(randomSeed);
                                int[] newBitVector = new int[bitVectorSize];
                                for (int q = 0;q < bitVectorSize;q++){
                                    int random1 = random.nextInt(bitVectorSize);
                                    int random2 = random.nextInt(bitVectorSize);
                                    if (bitVector[random1] == bitVector[random2]) newBitVector[q] = 0;
                                    else newBitVector[q] = 1;
                                }
                                bitVector = newBitVector;
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
        for (int i = 0; i < record.size(); i++) {
            String tempStr = record.get(i);
            if (isNumeric(tempStr)){
                for (int j = 0;j < tempStr.length();j++){
                    ngrams.add(String.valueOf(tempStr.charAt(j)));
                }
            }else{
                for (int j = 0;j < tempStr.length() - n + 1;j++){
                    ngrams.add(tempStr.substring(j, j + n));
                }
            }
        }

        return ngrams;
    }

    public boolean isNumeric(String str){
        for (int i = 0;i < str.length();i++){
            if (!Character.isDigit(str.charAt(i))){
                return false;
            }
        }
        return true;
    }

    public void calSimilarities(Map<String, List<List<String>>> dataset1, Map<String, List<List<String>>> dataset2,
                                Map<String, List<List<Integer>>> bitVectors1, Map<String, List<List<Integer>>> bitVectors2,
                                List<Double[]> similarities, int numGrams) {
        for (String key : dataset1.keySet()){
            List<List<String>> keyDataset2;
            List<List<Integer>> keyBitVector2;
            if (dataset2.containsKey(key)) {
                //第二个数据集有这个key才继续
                keyDataset2 = dataset2.get(key);
                keyBitVector2 = bitVectors2.get(key);
            }
            else continue;
            List<List<String>> keyDataset1 = dataset1.get(key);
            List<List<Integer>> keyBitVector1 = bitVectors1.get(key);

            int threadNum = 1;//线程数量
            if (keyDataset1.size() > 1000){
                threadNum = 5;
            }
            int keyDataset1Size = keyDataset1.size();
            int keyDataset2Size = keyDataset2.size();
            int numPerThread = Math.floorDiv(keyDataset1Size, threadNum) + 1;
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < threadNum; i++) {
                int start = i * numPerThread;
                int end = Math.min(start + numPerThread, keyDataset1Size);
                Thread thread = new Thread(() -> {
                    double sim;
                    double esSim;
                    for (int j = start; j < end; j++) {
                        for (int k = 0; k < keyDataset2Size; k++) {
                            sim = calSimilarity(keyDataset1.get(j), keyDataset2.get(k), numGrams);
                            esSim = esSimilarity(keyBitVector1.get(j), keyBitVector2.get(k));
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

    public double esSimilarity(List<Integer> bitVector1, List<Integer> bitVector2) {
        int identical = 0;
        int count1 = 0;
        int count2 = 0;
        for (int i = 0; i < Math.min(bitVector1.size(), bitVector2.size()); i++) {
            if (bitVector1.get(i) == 1) count1++;
            if (bitVector2.get(i) == 1) count2++;
            if (bitVector1.get(i) == 1 && bitVector2.get(i) == 1) identical++;
        }

        if ((count1 + count2) == 0) {
            return 0;
        }
        return (double) 2 * identical / (count1 + count2);
    }

    public void storeResult(String datasetName, String[] filenames, int datasetSize, int bitVectorSize,
                            int nGrams, int k, String[] attributes, String method,
                            List<Double[]> similarities, String blockingMethod) {
        String filepath = "StringResult/";
        for (String filename : filenames) {
            filepath = filepath + filename.substring(0, filename.length() - 4) + "-";
        }
        filepath  = filepath + "/" + method + "/" + blockingMethod + "/";
        for (String attribute : attributes) {
            filepath += attribute + "-";
        }
        File fileDir = new File(filepath);
        if (!fileDir.exists()) {
            if (!fileDir.mkdirs()) {
                System.out.println("创建文件夹失败");
                return;
            }
        }
        StringBuilder resultFile = new StringBuilder(datasetName);
        RandomResponse randomResponse = new RandomResponse();
        resultFile.append("-").append(datasetSize).append("-").append(bitVectorSize).append("-").append(nGrams)
                .append("-").append(k).append("-"). append(randomResponse.epsilon);

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

    public static void main(String[] args) {
        BloomFilter bloomFilter = new BloomFilter();
        String[] filenames = {"census.csv", "cis.csv"};
        String[][] attributes = {
                {"PERNAME1", "PERNAME2", "SEX"},
                {"PERNAME1", "PERNAME2", "SEX", "DOB_YEAR"},
                {"PERNAME1", "PERNAME2", "SEX", "DOB_YEAR" , "PERSON_ID"}};
        String key = "bloomfilter";
        int dataSize = 20000;
        int bitVectorSize = 1000;
        int nGrams = 2;
        int k = 30;
        String blockingMethod = "Soundex";
        for (String[] tempAttributes : attributes) {
            System.out.println("--------------------------" + Arrays.toString(tempAttributes) + "----------------------------");
            bloomFilter.experiment("Istat", filenames, dataSize, bitVectorSize, nGrams, k, tempAttributes,
                    "Normal", key, blockingMethod);
            bloomFilter.experiment("Istat", filenames, dataSize, bitVectorSize, nGrams, k, tempAttributes,
                    "ULDP", key, blockingMethod);
            bloomFilter.experiment("Istat", filenames, dataSize, bitVectorSize, nGrams, k, tempAttributes,
                    "WXOR", key, blockingMethod);
            bloomFilter.experiment("Istat", filenames, dataSize, bitVectorSize, nGrams, k, tempAttributes,
                    "RESAM", key, blockingMethod);
        }
    }
}
