package String.FreqAttack;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import String.EvalLinkage;

public class EvalAttack {
    private Map<String, List<Integer>> qgramPositions;
    public int q = 2;
    public boolean padded = false;
    public int k;
    public int bfLen = 1000;
    public static final String hashFunction1 = "SHA1";
    public static final String hashFunction2 = "MD5";
    public static final String PAD_CHAR = "_";
    public ThreadPoolExecutor threadPool;

    EvalAttack() {
        this.threadPool = new ThreadPoolExecutor(8, 10, 1, TimeUnit.DAYS,
                new LinkedBlockingDeque<Runnable>());
    }

    public Map<String, BitSet> loadBFs(String filePath, String datasetName, String encodeType, String hashType,
                                       String hardenType, String numHashFunctions) {
        bfLen = 1000;
        if (hardenType.equals("xor"))
            bfLen = bfLen * 2;
        String bfsFile = filePath + String.format("BF-%s-encode%s-hash%s-harden%s-k%s-bfLen%d.csv",
                datasetName, encodeType, hashType, hardenType, numHashFunctions, bfLen);

        Map<String, BitSet> bfs = new HashMap<>();
        try (FileReader fr = new FileReader(bfsFile);
             BufferedReader br = new BufferedReader(fr)) {
            br.readLine();
            String line = br.readLine();
            String[] lineArray;
            while (line != null) {
                BitSet bitSet = new BitSet(bfLen);
                lineArray = line.replace("\"", "").split(",");
                String[] positions = lineArray[1].replace("{", "").replace("}", "").split("  ");
                for (String pos : positions)
                    bitSet.set(Integer.parseInt(pos));
                bfs.put(lineArray[0], bitSet);
                line = br.readLine();
            }
            System.out.printf("      load bfs from : %s%n", bfsFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bfs;
    }

    public List<List<BitSet>> sortBFs(Map<String, BitSet> data, int freqT) {
        Map<BitSet, Integer> recFreqs = new HashMap<>();
        for (String entityID : data.keySet()) {
            BitSet recData = data.get(entityID);
            int freq = recFreqs.getOrDefault(recData, 0);
            freq += 1;
            recFreqs.put(recData, freq);
        }

        PriorityQueue<Map<BitSet, Integer>> recFreqQueue = new PriorityQueue<>(
                new Comparator<Map<BitSet, Integer>>() {
                    @Override
                    public int compare(Map<BitSet, Integer> o1, Map<BitSet, Integer> o2) {
                        return o2.values().iterator().next() - o1.values().iterator().next();
                    }
                });
        List<BitSet> recNonFreqList = new ArrayList<>();
        for (BitSet rec : recFreqs.keySet()) {
            int freq = recFreqs.get(rec);
            if (freq >= freqT) {
                Map<BitSet, Integer> map = new HashMap<>();
                map.put(rec, freq);
                recFreqQueue.add(map);
            }
            else recNonFreqList.add(rec);
        }

        List<List<BitSet>> res = new ArrayList();
        List<BitSet> recFreqList = new ArrayList<>();
        while (!recFreqQueue.isEmpty()){
            Map<BitSet, Integer> mapTemp = recFreqQueue.poll();
            recFreqList.add((BitSet) mapTemp.keySet().toArray()[0]);
        }
        res.add(recFreqList);
        res.add(recNonFreqList);
        return res;
    }

    public List<List<String[]>> sortAttrs(Map<String, List<String>> data, int freqT) {
        Map<List<String>, Integer> recFreqs = new HashMap<>();
        for (String entityID : data.keySet()) {
            List<String> recDataList = data.get(entityID);
            int freq = recFreqs.getOrDefault(recDataList, 0);
            freq += 1;
            recFreqs.put(recDataList, freq);
        }

        List<String[]> recFreqList = new ArrayList<>();
        List<String[]> recNonFreqList = new ArrayList<>();
        for (List<String> rec : recFreqs.keySet()) {
            int freq = recFreqs.get(rec);
            if (freq >= freqT) recFreqList.add(new String[]{EvalLinkage.strList2String(rec), String.valueOf(freq)});
            else recNonFreqList.add(new String[]{EvalLinkage.strList2String(rec), String.valueOf(freq)});
        }

        recFreqList.sort((o1, o2) -> o2[1].compareTo(o1[1]));
        List<List<String[]>> res = new ArrayList();
        res.add(recFreqList);
        res.add(recNonFreqList);
        return res;
    }

    public void getCandidateB(Map<BitSet, List<String>> bfAttrPair,
                              Map<Integer, Set<String>> candiProbB, Map<Integer, Set<String>> candiNoProbB,
                              Map<List<String>, List<String>> recAttrs2Qgrams) {
        int totalPairs = bfAttrPair.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<BitSet> bfAttrPairKeyList = bfAttrPair.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    BitSet bfPositions = bfAttrPairKeyList.get(j);
                    List<String> attrs = bfAttrPair.get(bfPositions);
                    List<String> qgrams = recAttrs2Qgrams.get(attrs);
                    for (int pos = 0; pos < bfLen; pos++) {
                        if (bfPositions.get(pos)) {
                            synchronized (candiProbB) {
                                Set<String> candiProb = candiProbB.getOrDefault(pos, new HashSet<>());
                                candiProb.addAll(qgrams);
                                candiProbB.put(pos, candiProb);
                            }
                        } else {
                            synchronized (candiNoProbB) {
                                Set<String> candiNoProb = candiNoProbB.getOrDefault(pos, new HashSet<>());
                                candiNoProb.addAll(qgrams);
                                candiNoProbB.put(pos, candiNoProb);
                            }
                        }
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Integer pos : candiProbB.keySet()) {
            if (candiNoProbB.containsKey(pos)) {
                candiProbB.get(pos).removeAll(candiNoProbB.get(pos));
            }
        }
    }

    public void getQgramPosAssign(Map<BitSet, List<String>> bfAttrPair, Map<Integer, Set<String>> candiProbB,
                                  Map<Integer, Set<String>> candiAssignB, Map<List<String>, List<String>> recAttrs2Qgrams) {
        int totalPairs = bfAttrPair.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<BitSet> bfAttrPairKeyList = bfAttrPair.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    BitSet bfPositions = bfAttrPairKeyList.get(j);
                    List<String> attrs = bfAttrPair.get(bfPositions);
                    List<String> qgrams = recAttrs2Qgrams.get(attrs);
                    for (int pos = 0; pos < bfLen; pos++) {
                        if (bfPositions.get(pos)) {
                            Set<String> candiProb = candiProbB.getOrDefault(pos, new HashSet<>());
                            Set<String> jointQgrams = new HashSet<>(qgrams);
                            jointQgrams.retainAll(candiProb);
                            if (jointQgrams.size() == 1) {
                                synchronized (candiAssignB) {
                                    Set<String> candiAssign = candiAssignB.getOrDefault(pos, new HashSet<>());
                                    candiAssign.addAll(jointQgrams);
                                    candiAssignB.put(pos, candiAssign);
                                }
                            }
                        }
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void qgramRefineAndExpend(Map<BitSet, List<String>> bfAttrPair, List<BitSet> sortedFreqBfs,
                                     List<String[]> sortedFreqAttrs, List<BitSet> nonFreqBfs, List<String[]> nonFreqAttrs,
                                     Map<Integer, Set<String>> candiProbR, Map<Integer, Set<String>> candiNoProbR,
                                     Map<Integer, Set<String>> candiAssignR, Map<List<String>, List<String>> recAttrs2Qgrams,
                                     int maxSize) {
        int totalPairs = bfAttrPair.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<BitSet> bfAttrPairKeyList = bfAttrPair.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    BitSet bf = bfAttrPairKeyList.get(j);
                    List<String> attr = bfAttrPair.get(bf);
                    List<String> qgrams = recAttrs2Qgrams.get(attr);

                    List<BitSet> BiS = new ArrayList<>();
                    List<BitSet> BiL = new ArrayList<>();
                    List<List<String>> ViS = new ArrayList<>();
                    List<List<String>> ViL = new ArrayList<>();

                    for (BitSet nonFreqBF : nonFreqBfs) {
                        BitSet nonFreqBFTemp = nonFreqBF.get(0, nonFreqBF.size());
                        nonFreqBFTemp.and(bf);
                        if (nonFreqBFTemp.equals(bf) && bf.cardinality() < nonFreqBF.cardinality()) BiL.add(nonFreqBF);
                        if (nonFreqBFTemp.equals(nonFreqBF) && nonFreqBF.cardinality() < bf.cardinality()) BiS.add(nonFreqBF);
                    }

                    for (String[] nonFreqAttr : nonFreqAttrs) {
                        String nonFreqAttrStr = nonFreqAttr[0];
                        List<String> nonFreqAttrList = Arrays.asList(nonFreqAttrStr.split(","));
                        List<String> nonFreqQgrams = recAttrs2Qgrams.get(nonFreqAttrList);
                        if (nonFreqQgrams.containsAll(qgrams)) ViL.add(nonFreqAttrList);
                        if (qgrams.containsAll(nonFreqQgrams)) ViS.add(nonFreqAttrList);
                    }

                    if (ViS.size() <= maxSize && ViS.size() > 0 && BiS.size() > 0 && BiS.size() <= maxSize) {
                        Set<String> qgramsU = new HashSet<>();
                        for (List<String> attrTemp : ViS) {
                            qgramsU.addAll(recAttrs2Qgrams.get(attrTemp));
                        }
                        Set<String> qgramsD = new HashSet<>(qgrams);
                        qgramsD.removeAll(qgramsU);
                        BitSet bfO = new BitSet(bfLen);
                        for (BitSet bfTemp : BiS) bfO.or(bfTemp);
                        if (qgramsD.size() > 0 && !bfO.equals(bf)) {
                            for (int pos = 0; pos < bfLen; pos++) {
                                if (!bfO.get(pos) && bf.get(pos)) {
                                    synchronized (candiProbR) {
                                        Set<String> set1 = candiProbR.getOrDefault(pos, new HashSet<>());
                                        set1.addAll(qgramsD);
                                        candiProbR.put(pos, set1);
                                    }
                                    synchronized (candiNoProbR) {
                                        Set<String> set2 = candiNoProbR.getOrDefault(pos, new HashSet<>());
                                        set2.addAll(qgramsU);
                                        candiNoProbR.put(pos, set2);
                                    }

                                    if (ViS.size() == 1 && BiS.size() == 1 && qgramsD.size() == 1) {
                                        synchronized (candiProbR) {
                                            Set<String> set3 = candiAssignR.getOrDefault(pos, new HashSet<>());
                                            set3.addAll(qgramsD);
                                            candiProbR.put(pos, set3);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (ViL.size() <= maxSize && ViL.size() > 0 && BiL.size() > 0 && BiL.size() <= maxSize) {
                        Set<String> qgramsU = new HashSet<>();
                        Set<String> qgramsS = new HashSet<>();
                        int times = 0;
                        for (List<String> attrTemp : ViL) {
                            qgramsU.addAll(recAttrs2Qgrams.get(attrTemp));
                            if (times == 0) qgramsS.addAll(recAttrs2Qgrams.get(attrTemp));
                            else qgramsS.retainAll(recAttrs2Qgrams.get(attrTemp));
                            times++;
                        }
                        Set<String> qgramsD = new HashSet<>(qgramsS);
                        qgrams.forEach(qgramsD::remove);
                        BitSet bfO = new BitSet(bfLen);
                        BitSet bfA = new BitSet(bfLen);
                        for (BitSet bfTemp : BiL) {
                            bfO.or(bfTemp);
                            bfA.and(bfTemp);
                        }
                        if (qgramsD.size() > 0 && !bfA.equals(bf)) {
                            for (int pos = 0; pos < bfLen; pos++) {
                                if (!bf.get(pos) && bfA.get(pos)) {
                                    synchronized (candiProbR) {
                                        Set<String> set1 = candiProbR.getOrDefault(pos, new HashSet<>());
                                        set1.addAll(qgramsD);
                                        candiProbR.put(pos, set1);
                                    }
                                    if (ViL.size() == 1 && BiL.size() == 1 && qgramsD.size() == 1) {
                                        synchronized (candiAssignR) {
                                            Set<String> set3 = candiAssignR.getOrDefault(pos, new HashSet<>());
                                            set3.addAll(qgramsD);
                                            candiAssignR.put(pos, set3);
                                        }
                                    }
                                } else if (!bfO.get(pos) && !bf.get(pos)) {
                                    synchronized (candiNoProbR) {
                                        Set<String> set2 = candiNoProbR.getOrDefault(pos, new HashSet<>());
                                        set2.addAll(qgramsU);
                                        candiNoProbR.put(pos, set2);
                                    }
                                }
                            }
                        }
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void getGNOrGP(Map<Integer, Set<String>> candiM, Set<List<String>> GNorGP,
                          Map<List<String>, List<String>> recAttrs2Qgrams) {
        Set<String> QNorQP = new HashSet<>();
        for (int pos : candiM.keySet()) {
            QNorQP.addAll(candiM.get(pos));
        }

        int totalPairs = recAttrs2Qgrams.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<List<String>> recAttr2QgramsKeyList = recAttrs2Qgrams.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    List<String> recAttrs = recAttr2QgramsKeyList.get(j);
                    List<String> qgrams = recAttrs2Qgrams.get(recAttrs);
                    for (String qgram : qgrams) {
                        if (QNorQP.contains(qgram)) {
                            synchronized (GNorGP) {
                                GNorGP.add(recAttrs);
                            }
                            break;
                        }
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Set<List<String>>> identifyOnProb(Map<String, BitSet> bfs, Map<Integer, Set<String>> candiProbM,
                                                         Set<List<String>> GP, Map<List<String>, List<String>> recAttrs2Qgrams) {
        Map<String, Set<List<String>>> R = new HashMap<>();

        int totalBFs = bfs.keySet().size();
        int threadNum = 8;
        int num_entity_per_thread = totalBFs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalBFs);
            List<String> bfsKeyList = bfs.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    String entityID = bfsKeyList.get(j);
                    BitSet bfPositions = bfs.get(entityID);
                    List<List<String>> G = new ArrayList<>(GP);
                    for (int pos = 0; pos < bfLen; pos++) {
                        if (bfPositions.get(pos)) {
                            Set<String> qp = candiProbM.get(pos);
                            for (int p = 0; p < G.size(); p++) {
                                List<String> recAttrs = G.get(p);
                                List<String> qgrams = recAttrs2Qgrams.get(recAttrs);
                                int cnt = 0;
                                for (String qgram : qp){
                                    if (qgrams.contains(qgram)) {
                                        cnt++;
                                        break;
                                    }
                                }
                                if (cnt == 0) {
                                    G.remove(recAttrs);
                                    p--;
                                }
                            }
                        }
                    }
                    synchronized (R) {
                        R.put(entityID, new HashSet<>(G));
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return R;
    }

    public Map<String, Set<List<String>>> identifyOnNoProb(Map<String, BitSet> bfs, Map<Integer, Set<String>> candiNoProbM,
                                                           Set<List<String>> GN, Map<List<String>, List<String>> recAttrs2Qgrams) {
        Set<String> QN = new HashSet<>();
        for (int pos : candiNoProbM.keySet()) {
            QN.addAll(candiNoProbM.get(pos));
        }

        Map<String, BitSet> BQ = new HashMap<>();
        Map<List<String>, BitSet> BG = new HashMap<>();
        for (String qgram : QN) {
            BQ.put(qgram, new BitSet(bfLen));
        }

        for (int pos = 0; pos < bfLen; pos++) {
            if (candiNoProbM.containsKey(pos)) {
                for (String qgram : candiNoProbM.get(pos)) {
                    BitSet posTemp = BQ.get(qgram);
                    posTemp.set(pos);
                    BQ.put(qgram, posTemp);
                }
            }
        }

        int totalRecsInGN = GN.size();
        int threadNum = 8;
        int num_entity_per_thread = totalRecsInGN / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalRecsInGN);
            List<List<String>> GNList = GN.stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    List<String> recAttrs = GNList.get(j);
                    List<String> qj = recAttrs2Qgrams.get(recAttrs);
                    if (QN.containsAll(qj)) {
                        BitSet posTemp = BG.getOrDefault(recAttrs, new BitSet(bfLen));
                        for (String qgram : qj) {
                            posTemp.and(BQ.get(qgram));
                        }
                        synchronized (BG) {
                            BG.put(recAttrs, posTemp);
                        }
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, Set<List<String>>> R = new HashMap<>();
        int totalBFs = bfs.keySet().size();
        num_entity_per_thread = totalBFs / threadNum + 1;
        CountDownLatch countDownLatch1 = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalBFs);
            List<String> bfKetList = bfs.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start;j < end;j++) {
                    String entityID = bfKetList.get(j);
                    BitSet bf = bfs.get(entityID);
                    Set<List<String>> G = new HashSet<>();
                    for (List<String> recAttrs : GN) {
                        if (BG.containsKey(recAttrs)){
                            BitSet posTemp = BG.get(recAttrs);
                            BitSet posTempTemp = posTemp.get(0, posTemp.size());
                            posTempTemp.and(bf);
                            if (posTempTemp.cardinality() == 0) G.add(recAttrs);
                        }
                    }
                    R.put(entityID, G);
                }
                countDownLatch1.countDown();
            });
        }
        try {
            countDownLatch1.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return R;
    }

    public List<String> qGramListForRec(List<String> attr_val_list) {
        List<String> qGramList = new ArrayList<>();
        for (String attr_val : attr_val_list) {
            qGramList.addAll(qGramListForAttr(attr_val, q, padded));
        }

        return qGramList;
    }

    public List<String> qGramListForAttr(String attr_val, int q, boolean padded) {
        List<String> qGramList = new ArrayList<>();
        int padded_num = q - 1;
        String qGram;
        if (isNumeric(attr_val)) {
            for (int j = 0; j < attr_val.length(); j++) {
                qGram = String.valueOf(attr_val.charAt(j));
                qGramList.add(qGram);
            }
        } else {
            if (padded) {
                attr_val = PAD_CHAR.repeat(Math.max(0, padded_num)) + attr_val
                        + PAD_CHAR.repeat(Math.max(0, padded_num));
            }
            for (int j = 0; j < attr_val.length() - q + 1; j++) {
                qGram = attr_val.substring(j, j + q);
                qGramList.add(qGram);
            }
        }

        return qGramList;
    }

    public boolean isNumeric(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        EvalAttack evalAttack = new EvalAttack();
        int freqT = 2;

        String attrFile = "/Users/takafumikai/dataset/ncvoter/ncvoter-s-50000-1.csv";
        String storeNoProbFile = "/Users/takafumikai/dataset/ncvoter/attackNoProb.csv";
        String storeProbFile = "/Users/takafumikai/dataset/ncvoter/attackProb.csv";
        String bfFilePath = "/Users/takafumikai/dataset/ncvoter/";
        String datasetName = "s-1-1";
        String dateDiff = "none";
        int[] attr_index_list = {1, 2};  // attr's columns used
        int entity_id_col = 0;

        EvalLinkage evalLinkage = new EvalLinkage(null, null, null);

        long startTime;
        long endTime;

        List<Map> result = evalLinkage.load_dataset_salt(attrFile, attr_index_list, entity_id_col, -1);
        Map<String, List<String>> rec_attr_val_dict = result.get(0);

        System.out.println("Generate qgrams for records");
        startTime = new Date().getTime();
        Map<List<String>, List<String>> recAttr2Qgrams = evalAttack.getQgramsForRec(rec_attr_val_dict);
        endTime = new Date().getTime();
        System.out.printf(" generate qgrams for record cost time: %d msec\n", endTime - startTime);

        System.out.println("Sort record attributes");
        startTime = new Date().getTime();
        List<List<String[]>> attrRes = evalAttack.sortAttrs(rec_attr_val_dict, freqT);
        List<String[]> sortedFreqAttrs = attrRes.get(0);
        List<String[]> nonFreqAttrs = attrRes.get(1);
        endTime = new Date().getTime();
        System.out.printf(" freq records size: %d / non freq records size: %d\n", sortedFreqAttrs.size(), nonFreqAttrs.size());
        System.out.printf(" sort record attributes cost time: %d msec\n", endTime - startTime);

        String[] hash_type_list = new String[]{"dh-false"};
        String[] num_hash_functions_list = {"10"};
        String[] encode_type_list = {"clk"};
        String[] bf_harden_types = new String[]{
                "none",
        };
        Map<String, BitSet> bfs;
        Map<String, List<Integer>> storeNoProbResult = new HashMap<>();
        Map<String, List<Integer>> storeProbResult = new HashMap<>();
        for (String hashType : hash_type_list) {
            for (String encodeType : encode_type_list) {
                for (String hardenType : bf_harden_types) {
                    for (String numHashFunctions : num_hash_functions_list) {
                        System.out.printf("Re-identify bf encode:%s hash:%s harden:%s k:%s\n", encodeType, hashType,
                                hardenType, numHashFunctions);
                        evalAttack.k = Integer.parseInt(numHashFunctions);
                        System.out.println("    Load bf file");
                        startTime = new Date().getTime();
                        bfs = evalAttack.loadBFs(bfFilePath, datasetName, encodeType, hashType, hardenType, numHashFunctions);
                        endTime = new Date().getTime();
                        System.out.printf("      load bf file cost time: %d msec\n", endTime - startTime);

                        System.out.println("    Sort Bloom Filters");
                        startTime = new Date().getTime();
                        List<List<BitSet>> bfsRes = evalAttack.sortBFs(bfs, freqT);
                        List<BitSet> sortedFreqBfs = bfsRes.get(0);
                        List<BitSet> nonFreqBfs = bfsRes.get(1);
                        endTime = new Date().getTime();
                        System.out.printf("      freq bfs size: %d / non freq bfs size: %d\n", sortedFreqBfs.size(),
                                nonFreqBfs.size());
                        System.out.printf("      sort bfs cost time: %d msec\n", endTime - startTime);

                        System.out.println("    Generate Bloom Filter and Record Pair by freq");
                        Map<BitSet, List<String>> bfAttrPair = new HashMap<>();
                        for (int i = 0; i < Math.min(sortedFreqBfs.size(), sortedFreqAttrs.size()); i++) {
                            String[] attrFreq = sortedFreqAttrs.get(i);
                            String[] attrs = attrFreq[0].split(",");
                            BitSet bfPositions = sortedFreqBfs.get(i);
                            bfAttrPair.put(bfPositions, Arrays.asList(attrs));
                        }
                        System.out.printf("      bf and rec pairs size: %d msec\n", bfAttrPair.size());

                        System.out.println("    Generate basic candidates(possible, no possible, assign)");
                        startTime = new Date().getTime();
                        Map<Integer, Set<String>> candiProbB = new HashMap<>();
                        Map<Integer, Set<String>> candiNoProbB = new HashMap<>();
                        evalAttack.getCandidateB(bfAttrPair, candiProbB, candiNoProbB, recAttr2Qgrams);
                        Map<Integer, Set<String>> candiAssignB = new HashMap<>();
                        evalAttack.getQgramPosAssign(bfAttrPair, candiProbB, candiAssignB, recAttr2Qgrams);
                        endTime = new Date().getTime();
                        System.out.printf("      generate basic candidates(possible, no possible, assign) cost time: %d msec\n",
                                endTime - startTime);

                        System.out.println("    Generate refined and expended candidates(possible, no possible, assign)");
                        startTime = new Date().getTime();
                        Map<Integer, Set<String>> candiProbR = new HashMap<>();
                        Map<Integer, Set<String>> candiNoProbR = new HashMap<>();
                        Map<Integer, Set<String>> candiAssignR = new HashMap<>();
                        evalAttack.qgramRefineAndExpend(bfAttrPair, sortedFreqBfs, sortedFreqAttrs, nonFreqBfs,
                                nonFreqAttrs, candiProbR, candiNoProbR, candiAssignR, recAttr2Qgrams,
                                Math.max(bfs.size(), rec_attr_val_dict.size()));
                        endTime = new Date().getTime();
                        System.out.printf("      generate refined and expended candidates(possible, no possible, assign) cost time: %d msec\n",
                                endTime - startTime);

                        System.out.println("    Generate merged candidates(possible, no possible, assign)");
                        Map<Integer, Set<String>> candiProbM = new HashMap<>();
                        Map<Integer, Set<String>> candiNoProbM = new HashMap<>();
                        Map<Integer, Set<String>> candiAssignM = new HashMap<>();
                        for (int pos = 0; pos < evalAttack.bfLen; pos++) {
                            Set<String> set2 = new HashSet<>();
                            if (candiNoProbB.containsKey(pos)) set2.addAll(candiNoProbB.get(pos));
                            if (candiNoProbR.containsKey(pos)) set2.addAll(candiNoProbR.get(pos));
                            candiNoProbM.put(pos, set2);

                            Set<String> set1 = new HashSet<>();
                            if (candiProbB.containsKey(pos)) set1.addAll(candiProbB.get(pos));
                            if (candiProbR.containsKey(pos)) set1.addAll(candiProbR.get(pos));
                            set1.removeAll(candiNoProbM.get(pos));
                            candiProbM.put(pos, set1);

                            Set<String> set3 = new HashSet<>();
                            if (candiAssignB.containsKey(pos)) set3.addAll(candiAssignB.get(pos));
                            if (candiAssignR.containsKey(pos)) set3.addAll(candiAssignR.get(pos));
                            set3.removeAll(candiNoProbM.get(pos));
                            candiAssignM.put(pos, set3);
                        }

                        System.out.println("    Generate GN and GP");
                        startTime = new Date().getTime();
                        Set<List<String>> GN = new HashSet<>();
                        Set<List<String>> GP = new HashSet<>();
                        evalAttack.getGNOrGP(candiNoProbM, GN, recAttr2Qgrams);
                        evalAttack.getGNOrGP(candiProbM, GP, recAttr2Qgrams);
                        endTime = new Date().getTime();
                        System.out.printf("      generate GN and GP cost time: %d msec\n", endTime - startTime);

                        System.out.println("    Re-identify on possible candidates");
                        startTime = new Date().getTime();
                        Map<String, Set<List<String>>> ROnProb = evalAttack.identifyOnProb(bfs, candiProbM, GP, recAttr2Qgrams);
                        endTime = new Date().getTime();
                        System.out.printf("      re-identify on possible candidates cost time: %d msec\n", endTime - startTime);

                        System.out.println("    Re-identify on no possible candidates");
                        startTime = new Date().getTime();
                        Map<String, Set<List<String>>> ROnNoProb = evalAttack.identifyOnNoProb(bfs, candiNoProbM, GN, recAttr2Qgrams);
                        endTime = new Date().getTime();
                        System.out.printf("      re-identify on no possible candidates cost time: %d msec\n", endTime - startTime);

                        String method = String.format("%s-encode%s-hash%s-harden%s-k%s-bfLen%d", datasetName,
                                encodeType, hashType, hardenType, numHashFunctions, evalAttack.bfLen);
                        startTime = new Date().getTime();
                        List<Integer> resultProb = evalAttack.statistic(rec_attr_val_dict, ROnProb);
                        storeProbResult.put(method, resultProb);


                        List<Integer> resultNoProb = evalAttack.statistic(rec_attr_val_dict, ROnNoProb);
                        storeNoProbResult.put(method, resultNoProb);
                        endTime = new Date().getTime();
                        System.out.printf("    Re-identify on possible candidates result: 1-1:%d 1-m:%d w:%d\n",
                                resultProb.get(0), resultProb.get(1), resultProb.get(2));
                        System.out.printf("    Re-identify on no possible candidates result: 1-1:%d 1-m:%d w:%d\n",
                                resultNoProb.get(0), resultNoProb.get(1), resultNoProb.get(2));
                        System.out.printf("  Re-identify bf encode:%s hash:%s harden:%s k:%s cost time: %d msec\n\n", encodeType, hashType,
                                hardenType, numHashFunctions, endTime - startTime);
                    }
                }
            }
        }
        evalAttack.storeResult2File(storeProbFile, storeProbResult);
        System.out.printf("Store result on possible candidates in %s, result size: %d\n", storeProbFile, storeProbResult.size());
        evalAttack.storeResult2File(storeNoProbFile, storeNoProbResult);
        System.out.printf("Store result on no possible candidates in %s, result size: %d\n", storeNoProbFile, storeProbResult.size());

        evalAttack.threadPool.shutdown();
    }

    public List<Integer> statistic(Map<String, List<String>> recAttrsDict, Map<String, Set<List<String>>> R) {
        int one2one = 0;
        int one2many = 0;
        int wrong = 0;
        for (String entityID : recAttrsDict.keySet()) {
            List<String> recAttrs = recAttrsDict.get(entityID);
            int cnt = 0;
            for (List<String> recAttrsG : R.get(entityID)) {
                if (recAttrs.equals(recAttrsG)) {
                    cnt += 1;
                }
            }
            if (cnt > 0) {
                if (cnt == R.get(entityID).size()) one2one += 1;
                else one2many += 1;
            } else wrong += 1;
        }
        return Arrays.asList(one2one, one2many, wrong);
    }

    private Map<List<String>, List<String>> getQgramsForRec(Map<String, List<String>> rec_attr_val_dict) {
        Map<List<String>, List<String>> recAttrs2Qgrams = new HashMap<>();
        for (List<String> recAttrs : rec_attr_val_dict.values()) {
            List<String> qgrams = qGramListForRec(recAttrs);
            recAttrs2Qgrams.put(recAttrs, qgrams);
        }
        return recAttrs2Qgrams;
    }

    private void storeResult2File(String storeFile, Map<String, List<Integer>> storeResult) {
        File file = new File(storeFile);
        try (FileWriter fw = new FileWriter(file, false);
             BufferedWriter bw = new BufferedWriter(fw)) {
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    System.out.printf("创建文件%s失败\n", storeFile);
                    return;
                }
            }

            String line = "method,one2one,one2many,wrong";
            bw.write(line + "\n");
            for (String method : storeResult.keySet()) {
                List<Integer> list = storeResult.get(method);
                bw.write(String.format("%s,%d,%d,%d\n", method, list.get(0), list.get(1), list.get(2)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
