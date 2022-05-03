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

    public Map<String, String> loadBFs(String filePath, String datasetName, String encodeType, String hashType,
                                       String hardenType, String numHashFunctions) {
        int bfLen_ = 1000;
        if (hardenType.equals("xor"))
            bfLen_ = bfLen * 2;
        String bfsFile = filePath + String.format("BF-%s-encode%s-hash%s-harden%s-k%s-bfLen%d.csv",
                datasetName, encodeType, hashType, hardenType, numHashFunctions, bfLen_);

        Map<String, String> bfs = new HashMap<>();
        try (FileReader fr = new FileReader(bfsFile);
             BufferedReader br = new BufferedReader(fr)) {
            br.readLine();
            String line = br.readLine();
            String[] lineArray;
            while (line != null) {
                lineArray = line.replace("\"", "").split(",");
                bfs.put(lineArray[0], lineArray[1].replace("{", "").replace("}", ""));
                line = br.readLine();
            }
            System.out.printf("load bfs from : %s%n", bfsFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bfs;
    }

    public List<List<String[]>> sortBFs(Map<String, String> data, int freqT) {
        Map<String, Integer> recFreqs = new HashMap<>();
        for (String entityID : data.keySet()) {
            String recData = data.get(entityID);
            int freq = recFreqs.getOrDefault(recData, 0);
            freq += 1;
            recFreqs.put(recData, freq);
        }

        List<String[]> recFreqList = new ArrayList<>();
        List<String[]> recNonFreqList = new ArrayList<>();
        for (String rec : recFreqs.keySet()) {
            int freq = recFreqs.get(rec);
            if (freq >= freqT) recFreqList.add(new String[]{rec, String.valueOf(freq)});
            else recNonFreqList.add(new String[]{rec, String.valueOf(freq)});
        }

        recFreqList.sort((o1, o2) -> o2[1].compareTo(o1[1]));
        List<List<String[]>> res = new ArrayList();
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

    public void getCandidateB(Map<List<String>, List<String>> bfAttrPair,
                              Map<String, Set<String>> candiProbB, Map<String, Set<String>> candiNoProbB,
                              Map<List<String>, List<String>> recAttrs2Qgrams) {
        int totalPairs = bfAttrPair.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<List<String>> bfAttrPairKeyList = bfAttrPair.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    List<String> bfPositions = bfAttrPairKeyList.get(j);
                    List<String> attrs = bfAttrPair.get(bfPositions);
                    List<String> qgrams = recAttrs2Qgrams.get(attrs);
                    for (int k = 0; k < bfLen; k++) {
                        String pos = String.valueOf(k);
                        if (bfPositions.contains(pos)) {
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

        for (String pos : candiProbB.keySet()) {
            if (candiNoProbB.containsKey(pos)) {
                candiProbB.get(pos).removeAll(candiNoProbB.get(pos));
            }
        }
    }

    public void getQgramPosAssign(Map<List<String>, List<String>> bfAttrPair, Map<String, Set<String>> candiProbB,
                                  Map<String, Set<String>> candiAssignB, Map<List<String>, List<String>> recAttrs2Qgrams) {
        int totalPairs = bfAttrPair.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<List<String>> bfAttrPairKeyList = bfAttrPair.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    List<String> bfPositions = bfAttrPairKeyList.get(j);
                    List<String> attrs = bfAttrPair.get(bfPositions);
                    List<String> qgrams = recAttrs2Qgrams.get(attrs);
                    for (int k = 0; k < bfLen; k++) {
                        String pos = String.valueOf(k);
                        if (bfPositions.contains(pos)) {
                            Set<String> candiProb = candiProbB.getOrDefault(pos, new HashSet<>());
                            Set<String> jointQgrams = new HashSet<>();
                            for (String qgram : qgrams) {
                                if (candiProb.add(qgram)) jointQgrams.add(qgram);
                                if (jointQgrams.size() > 1) break;
                            }
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

    public void qgramRefineAndExpend(Map<List<String>, List<String>> bfAttrPair, List<String[]> sortedFreqBfs,
                                     List<String[]> sortedFreqAttrs, List<String[]> nonFreqBfs, List<String[]> nonFreqAttrs,
                                     Map<String, Set<String>> candiProbR, Map<String, Set<String>> candiNoProbR,
                                     Map<String, Set<String>> candiAssignR, Map<List<String>, List<String>> recAttrs2Qgrams) {

        int m = Math.max(sortedFreqBfs.size() + nonFreqBfs.size(), sortedFreqAttrs.size() + nonFreqAttrs.size());

        int totalPairs = bfAttrPair.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<List<String>> bfAttrPairKeyList = bfAttrPair.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    List<String> bf = bfAttrPairKeyList.get(j);
                    List<List<String>> BiS = new ArrayList<>();
                    List<List<String>> BiL = new ArrayList<>();
                    List<List<String>> ViS = new ArrayList<>();
                    List<List<String>> ViL = new ArrayList<>();

                    for (String[] nonFreqBF : nonFreqBfs) {
                        String nonFreqBFStr = nonFreqBF[0];
                        List<String> nonFreqBFList = Arrays.asList(nonFreqBFStr.split("  "));
                        if (nonFreqBFList.containsAll(bf) && bf.size() < nonFreqBFList.size()) BiL.add(nonFreqBFList);
                        if (bf.containsAll(nonFreqBFList) && nonFreqBFList.size() < bf.size()) BiS.add(nonFreqBFList);
                    }

                    List<String> attr = bfAttrPair.get(bf);
                    List<String> qgrams = recAttrs2Qgrams.get(attr);
                    for (String[] nonFreqAttr : nonFreqAttrs) {
                        String nonFreqAttrStr = nonFreqAttr[0];
                        List<String> nonFreqAttrList = Arrays.asList(nonFreqAttrStr.split(","));
                        List<String> nonFreqQgrams = recAttrs2Qgrams.get(nonFreqAttrList);
                        if (nonFreqQgrams.containsAll(qgrams)) ViL.add(nonFreqAttrList);
                        if (qgrams.containsAll(nonFreqQgrams)) ViS.add(nonFreqAttrList);
                    }

                    if (ViS.size() <= m && ViS.size() > 0 && BiS.size() > 0 && BiS.size() <= m) {
                        Set<String> qgramsU = new HashSet<>();
                        for (List<String> attrTemp : ViS) {
                            qgramsU.addAll(recAttrs2Qgrams.get(attrTemp));
                        }
                        Set<String> qgramsD = new HashSet<>(qgrams);
                        qgramsD.removeAll(qgramsU);
                        Set<String> bfO = new HashSet<>();
                        for (List<String> bfTemp : BiS) {
                            bfO.addAll(bfTemp);
                        }
                        if (qgramsD.size() > 0 && !bfO.equals(new HashSet<>(bf))) {
                            for (int k = 0; k < bfLen; k++) {
                                String pos = String.valueOf(k);
                                if (!bfO.contains(pos) && bf.contains(pos)) {
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

                    if (ViL.size() <= m && ViL.size() > 0 && BiL.size() > 0 && BiL.size() <= m) {
                        Set<String> qgramsU = new HashSet<>();
                        for (List<String> attrTemp : ViL) {
                            qgramsU.addAll(recAttrs2Qgrams.get(attrTemp));
                        }
                        Set<String> qgramsS = new HashSet<>();
                        for (List<String> attrTemp : ViL) {
                            if (qgramsS.size() == 0) qgramsS.addAll(recAttrs2Qgrams.get(attrTemp));
                            else qgramsS.retainAll(recAttrs2Qgrams.get(attrTemp));
                        }
                        Set<String> qgramsD = new HashSet<>(qgramsS);
                        qgrams.forEach(qgramsD::remove);
                        Set<String> bfO = new HashSet<>();
                        for (List<String> bfTemp : BiL) {
                            bfO.addAll(bfTemp);
                        }
                        Set<String> bfA = new HashSet<>();
                        for (List<String> bfTemp : BiL) {
                            if (bfA.size() == 0) bfA.addAll(bfTemp);
                            else bfO.retainAll(bfTemp);
                        }
                        if (qgramsD.size() > 0 && !bfA.equals(new HashSet<>(bf))) {
                            for (int k = 0; k < bfLen; k++) {
                                String pos = String.valueOf(k);
                                if (!bf.contains(pos) && bfA.contains(pos)) {
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
                                } else if (!bfO.contains(pos) && !bf.contains(pos)) {
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

    public void getGNOrGP(Map<String, List<String>> attrs, Map<String, Set<String>> candiM, Set<List<String>> GNorGP,
                          Map<List<String>, List<String>> recAttrs2Qgrams) {
        Set<String> QN = new HashSet<>();
        for (String pos : candiM.keySet()) {
            if (candiM.containsKey(pos)) QN.addAll(candiM.get(pos));
        }

        Map<List<String>, List<String>> recAttr2Qgrams = new HashMap<>();
        for (List<String> recAttrs : attrs.values()) {
            recAttr2Qgrams.put(recAttrs, recAttrs2Qgrams.get(recAttrs));
        }

        int totalPairs = recAttr2Qgrams.keySet().size();
        int threadNum = Runtime.getRuntime().availableProcessors() + 1;
        int num_entity_per_thread = totalPairs / threadNum + 1;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum); // execute all the threads, then continue
        for (int i = 0; i < threadNum; i++) {
            int start = i * num_entity_per_thread;
            int end = Math.min(start + num_entity_per_thread, totalPairs);
            List<List<String>> recAttr2QgramsKeyList = recAttr2Qgrams.keySet().stream().toList();
            threadPool.execute(() -> {
                for (int j = start; j < end; j++) {
                    List<String> recAttrs = recAttr2QgramsKeyList.get(j);
                    List<String> listTemp = recAttr2Qgrams.get(recAttrs);
                    for (String qgram : QN) {
                        if (listTemp.contains(qgram)) {
                            synchronized (GNorGP) {
                                GNorGP.add(recAttrs);
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

    public Map<String, Set<List<String>>> identifyOnProb(Map<String, String> bfs, Map<String, Set<String>> candiProbM,
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
                    String bf = bfs.get(entityID);
                    List<String> bfPositions = Arrays.asList(bf.split("  "));
                    List<List<String>> G = new ArrayList<>(GP);
                    for (int k = 0; k < bfLen; k++) {
                        String pos = String.valueOf(k);
                        if (bfPositions.contains(pos)) {
                            Set<String> qp = candiProbM.get(pos);
                            for (int p = 0; p < G.size(); p++) {
                                List<String> recAttrs = G.get(p);
                                List<String> qgrams = recAttrs2Qgrams.get(recAttrs);
                                if (!qgrams.containsAll(qp)) {
                                    G.remove(p);
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

    public Map<String, Set<List<String>>> identifyOnNoProb(Map<String, String> bfs, Map<String, Set<String>> candiNoProbM,
                                                           Set<List<String>> GN, Map<List<String>, List<String>> recAttrs2Qgrams) {
        Set<String> QN = new HashSet<>();
        for (String pos : candiNoProbM.keySet()) {
            if (candiNoProbM.containsKey(pos)) QN.addAll(candiNoProbM.get(pos));
        }

        Map<String, List<String>> BQ = new HashMap<>();
        Map<List<String>, List<String>> BG = new HashMap<>();
        for (String qgram : QN) {
            BQ.put(qgram, new ArrayList<>());
        }

        for (int i = 0; i < bfLen; i++) {
            String pos = String.valueOf(i);
            if (candiNoProbM.containsKey(pos)) {
                for (String qgram : candiNoProbM.get(pos)) {
                    List<String> posTemp = BQ.get(qgram);
                    posTemp.add(pos);
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
                        List<String> posTemp = BG.getOrDefault(recAttrs, new ArrayList<>());
                        for (String qgram : qj) {
                            if (posTemp.size() == 0) posTemp.addAll(BQ.get(qgram));
                            else posTemp.retainAll(BQ.get(qgram));
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
        for (String entityID : bfs.keySet()) {
            String bf = bfs.get(entityID);
            Set<List<String>> G = new HashSet<>();
            for (List<String> recAttrs : GN) {
                List<String> posTemp = BG.get(recAttrs);
                posTemp.removeAll(Arrays.asList(bf.split("  ")));
                if (posTemp.size() == 0) G.add(recAttrs);
            }
            R.put(entityID, G);
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
        List<Map> result = evalLinkage.load_dataset_salt(attrFile, attr_index_list, entity_id_col, -1);
        Map<String, List<String>> rec_attr_val_dict = result.get(0);
        Map<List<String>, List<String>> recAttr2Qgrams = evalAttack.getQgramsForRec(rec_attr_val_dict);
        List<List<String[]>> attrRes = evalAttack.sortAttrs(rec_attr_val_dict, freqT);
        List<String[]> sortedFreqAttrs = attrRes.get(0);
        List<String[]> nonFreqAttrs = attrRes.get(1);
        String[] hash_type_list = new String[]{"dh-false", "dh-true", "rh-false", "rh-true"};
        String[] num_hash_functions_list = {"10", "20", "30"};
        String[] encode_type_list = {"clk"};
        String[] bf_harden_types = new String[]{
                "none",
                "salt",
                "bal",
                "rxor",
                "wxor-2",
                "blip-0.05",
                "blip-0.02",
                "blip-0.1",
                "urap-0.1-4",
                "urap-0.1-5",
                "urap-0.1-6",
                "urap-0.1-7",
                "urap-0.1-8",
                "urap-0.1-9",
                "urap-0.2-4",
                "urap-0.2-5",
                "urap-0.2-6",
                "urap-0.2-7",
                "urap-0.2-8",
                "urap-0.2-9",
                "urap-0.3-4",
                "urap-0.3-5",
                "urap-0.3-6",
                "urap-0.3-7",
                "urap-0.3-8",
                "urap-0.3-9",
                "urap-0.4-4",
                "urap-0.4-5",
                "urap-0.4-6",
                "urap-0.4-7",
                "urap-0.4-8",
                "urap-0.4-9",
                "urap-0.5-4",
                "urap-0.5-5",
                "urap-0.5-6",
                "urap-0.5-7",
                "urap-0.5-8",
                "urap-0.5-9",
                "urap-0.6-4",
                "urap-0.6-5",
                "urap-0.6-6",
                "urap-0.6-7",
                "urap-0.6-8",
                "urap-0.6-9",
                "urap-0.7-4",
                "urap-0.7-5",
                "urap-0.7-6",
                "urap-0.7-7",
                "urap-0.7-8",
                "urap-0.7-9",
                "urap-0.8-4",
                "urap-0.8-5",
                "urap-0.8-6",
                "urap-0.8-7",
                "urap-0.8-8",
                "urap-0.8-9",
                "urap-0.9-4",
                "urap-0.9-5",
                "urap-0.9-6",
                "urap-0.9-7",
                "urap-0.9-8",
                "urap-0.9-9",
                "indexD-0.2,0.01,0.05,0.02,0.001",
                "indexD-0.1,0.05,0.02,0.01,0.001"
        };
        Map<String, String> bfs;
        Map<String, List<Integer>> storeNoProbResult = new HashMap<>();
        Map<String, List<Integer>> storeProbResult = new HashMap<>();
        for (String hashType : hash_type_list) {
            for (String encodeType : encode_type_list) {
                for (String hardenType : bf_harden_types) {
                    for (String numHashFunctions : num_hash_functions_list) {
                        evalAttack.k = Integer.parseInt(numHashFunctions);
                        bfs = evalAttack.loadBFs(bfFilePath, datasetName, encodeType, hashType, hardenType, numHashFunctions);
                        List<List<String[]>> bfsRes = evalAttack.sortBFs(bfs, freqT);
                        List<String[]> sortedFreqBfs = bfsRes.get(0);
                        List<String[]> nonFreqBfs = bfsRes.get(1);
                        Map<List<String>, List<String>> bfAttrPair = new HashMap<>();
                        for (int i = 0; i < Math.min(sortedFreqBfs.size(), sortedFreqAttrs.size()); i++) {
                            String[] bfFreq = sortedFreqBfs.get(i);
                            String[] attrFreq = sortedFreqAttrs.get(i);
                            String[] bfPositions = bfFreq[0].split("  ");
                            String[] attrs = attrFreq[0].split(",");
                            bfAttrPair.put(Arrays.asList(bfPositions), Arrays.asList(attrs));
                        }

                        Map<String, Set<String>> candiProbB = new HashMap<>();
                        Map<String, Set<String>> candiNoProbB = new HashMap<>();
                        evalAttack.getCandidateB(bfAttrPair, candiProbB, candiNoProbB, recAttr2Qgrams);

                        Map<String, Set<String>> candiAssignB = new HashMap<>();
                        evalAttack.getQgramPosAssign(bfAttrPair, candiProbB, candiAssignB, recAttr2Qgrams);

                        Map<String, Set<String>> candiProbR = new HashMap<>();
                        Map<String, Set<String>> candiNoProbR = new HashMap<>();
                        Map<String, Set<String>> candiAssignR = new HashMap<>();
                        evalAttack.qgramRefineAndExpend(bfAttrPair, sortedFreqBfs, sortedFreqAttrs, nonFreqBfs,
                                nonFreqAttrs, candiProbR, candiNoProbR, candiAssignR, recAttr2Qgrams);

                        Map<String, Set<String>> candiProbM = new HashMap<>();
                        Map<String, Set<String>> candiNoProbM = new HashMap<>();
                        Map<String, Set<String>> candiAssignM = new HashMap<>();
                        for (int i = 0; i < evalAttack.bfLen; i++) {
                            String pos = String.valueOf(i);
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

                        Set<List<String>> GN = new HashSet<>();
                        Set<List<String>> GP = new HashSet<>();
                        evalAttack.getGNOrGP(rec_attr_val_dict, candiNoProbM, GN, recAttr2Qgrams);
                        evalAttack.getGNOrGP(rec_attr_val_dict, candiProbM, GP, recAttr2Qgrams);
                        Map<String, Set<List<String>>> ROnProb = evalAttack.identifyOnProb(bfs, candiProbM, GP, recAttr2Qgrams);
                        Map<String, Set<List<String>>> ROnNoProb = evalAttack.identifyOnNoProb(bfs, candiNoProbM, GN, recAttr2Qgrams);


                        String method = String.format("BF-%s-encode%s-hash%s-harden%s-k%s-bfLen%d", datasetName,
                                encodeType, hashType, hardenType, numHashFunctions, evalAttack.bfLen);
                        storeProbResult.put(method, evalAttack.statistic(rec_attr_val_dict, ROnProb));
                        storeNoProbResult.put(method, evalAttack.statistic(rec_attr_val_dict, ROnNoProb));
                    }
                }
            }
        }
        evalAttack.storeResult2File(storeProbFile, storeProbResult);
        evalAttack.storeResult2File(storeNoProbFile, storeNoProbResult);
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
