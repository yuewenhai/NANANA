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
    public int l = 1000;
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
        l = 1000;
        String bfsFile = filePath + String.format("BF-%s-encode%s-hash%s-harden%s-k%s-bfLen%d.csv",
                datasetName, encodeType, hashType, hardenType, numHashFunctions, l);

        Map<String, BitSet> bfs = new HashMap<>();
        try (FileReader fr = new FileReader(bfsFile);
             BufferedReader br = new BufferedReader(fr)) {
            br.readLine();
            String line = br.readLine();
            String[] lineArray;
            while (line != null) {
                BitSet bitSet = new BitSet(l);
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
        Map<Integer, List<BitSet>> bfsGroupByCard = new HashMap<>();
        for (String entityID : data.keySet()) {
            // 按照HW分组
            BitSet recData = data.get(entityID);
            int cardinality = recData.cardinality();
            List<BitSet> bitSets = bfsGroupByCard.getOrDefault(cardinality, new ArrayList<>());
            bitSets.add(recData);
            bfsGroupByCard.put(cardinality, bitSets);
        }

        PriorityQueue<List<BitSet>> bfsSortedByFreq = new PriorityQueue<>(
                new Comparator<List<BitSet>>() {
                    @Override
                    public int compare(List<BitSet> o1, List<BitSet> o2) {
                        return o2.size() - o1.size();
                    }
                });

        int lessFreqT = 0;
        for (int card : bfsGroupByCard.keySet()){
            List<BitSet> bfs = bfsGroupByCard.get(card);
            if (bfs.size() >= freqT) {
                bfsSortedByFreq.add(bfs);
            }else lessFreqT+=bfs.size();
        }
        System.out.printf("      bf less than freq threshold(%d) is: %d\n", freqT, lessFreqT);

        List<BitSet> bfsNonFreqList = new ArrayList<>();
        List<BitSet> bfsUniqueFreqList = new ArrayList<>();
        if (!bfsSortedByFreq.isEmpty()){
            List<BitSet> preBfs = bfsSortedByFreq.poll();
            int cnt = 1;
            while (!bfsSortedByFreq.isEmpty()){
                List<BitSet> bfs = bfsSortedByFreq.poll();
                if (bfs.size() == preBfs.size()){
                    bfsNonFreqList.addAll(preBfs);
                    cnt++;
                }else{
                    if (cnt == 1) {
                        bfsUniqueFreqList.addAll(preBfs);
                    } else {
                        bfsNonFreqList.addAll(preBfs);
                    }
                    cnt = 1;
                }
                preBfs = bfs;
            }
        }

        List<List<BitSet>> res = new ArrayList<>();
        res.add(bfsUniqueFreqList);
        res.add(bfsNonFreqList);
        return res;
    }

    public List<List<List<String>>> sortAttrs(Map<String, List<String>> data, int freqT) {
        Map<List<String>, List<List<String>>> recFreqs = new HashMap<>();
        for (String entityID : data.keySet()) {
            List<String> recDataList = data.get(entityID);
            List<List<String>> recs = recFreqs.getOrDefault(recDataList, new ArrayList<>());
            recs.add(recDataList);
            recFreqs.put(recDataList, recs);
        }

        PriorityQueue<List<List<String>>> recsSortedByFreq = new PriorityQueue<>(
                new Comparator<List<List<String>>>() {
                    @Override
                    public int compare(List<List<String>> o1, List<List<String>> o2) {
                        return o2.size() - o1.size();
                    }
                });

        int lessFreqT = 0;
        for (List<String> rec : recFreqs.keySet()){
            List<List<String>> recs = recFreqs.get(rec);
            if (recs.size() >= freqT) recsSortedByFreq.add(recs);
            else lessFreqT+=recs.size();
        }
        System.out.printf("      record less than freq threshold(%d) is: %d\n", freqT, lessFreqT);

        List<List<String>> recsNonFreqList = new ArrayList<>();
        List<List<String>> recsUniqueFreqList = new ArrayList<>();
        if (!recsSortedByFreq.isEmpty()){
            List<List<String>> preRecs = recsSortedByFreq.poll();
            int cnt = 1;
            while (!recsSortedByFreq.isEmpty()){
                List<List<String>> recs = recsSortedByFreq.poll();
                if (recs.size() == preRecs.size()){
                    recsNonFreqList.add(preRecs.get(0));
                    cnt++;
                }else{
                    if (cnt == 1) {
                        recsUniqueFreqList.add(preRecs.get(0));
                    } else {
                        recsNonFreqList.add(preRecs.get(0));
                    }
                    cnt = 1;
                }
                preRecs = recs;
            }
        }

        List<List<List<String>>> res = new ArrayList<>();
        res.add(recsUniqueFreqList);
        res.add(recsNonFreqList);
        return res;
    }

    public void getCandidateB(Map<BitSet, List<String>> bfAttrPair,
                              Map<Integer, Set<String>> CPb, Map<Integer, Set<String>> CNb,
                              Map<List<String>, HashSet<String>> recAttrs2Qgrams) {
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
                    List<String> attrs = bfAttrPair.get(bf);
                    HashSet<String> qgrams = recAttrs2Qgrams.get(attrs);
                    for (int pos = 0; pos < l; pos++) {
                        if (bf.get(pos)) {
                            synchronized (CPb) {
                                Set<String> candiProb = CPb.getOrDefault(pos, new HashSet<>());
                                candiProb.addAll(qgrams);
                                CPb.put(pos, candiProb);
                            }
                        } else {
                            synchronized (CNb) {
                                Set<String> candiNoProb = CNb.getOrDefault(pos, new HashSet<>());
                                candiNoProb.addAll(qgrams);
                                CNb.put(pos, candiNoProb);
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

        for (Integer pos : CPb.keySet()) {
            if (CNb.containsKey(pos)) {
                CPb.get(pos).removeAll(CNb.get(pos));
            }
        }
    }

    public void getQgramPosAssign(Map<BitSet, List<String>> bfAttrPair, Map<Integer, Set<String>> candiProbB,
                                  Map<Integer, Set<String>> candiAssignB, Map<List<String>, HashSet<String>> recAttrs2Qgrams) {
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
                    List<String> attrs = bfAttrPair.get(bf);
                    HashSet<String> qgrams = recAttrs2Qgrams.get(attrs);
                    for (int pos = 0; pos < l; pos++) {
                        if (bf.get(pos)) {
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

    public void qgramRefineAndExpend(Map<BitSet, List<String>> bfAttrPair, List<BitSet> nonFreqBfs, List<List<String>> nonFreqAttrs,
                                     Map<Integer, Set<String>> CPr, Map<Integer, Set<String>> CNr,
                                     Map<Integer, Set<String>> CAr, Map<List<String>, HashSet<String>> recAttrs2Qgrams,
                                     int m) {
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
                    BitSet bi = bfAttrPairKeyList.get(j);
                    List<String> attr = bfAttrPair.get(bi);
                    HashSet<String> qi = recAttrs2Qgrams.get(attr);

                    List<BitSet> BiS = new ArrayList<>();
                    List<BitSet> BiL = new ArrayList<>();
                    List<List<String>> ViS = new ArrayList<>();
                    List<List<String>> ViL = new ArrayList<>();

                    for (BitSet nonFreqBF : nonFreqBfs) {
                        BitSet nonFreqBFTemp = nonFreqBF.get(0, nonFreqBF.size());
                        nonFreqBFTemp.and(bi);
                        if (nonFreqBFTemp.equals(bi) && bi.cardinality() < nonFreqBF.cardinality()) BiL.add(nonFreqBF);
                        if (nonFreqBFTemp.equals(nonFreqBF) && nonFreqBF.cardinality() < bi.cardinality())
                            BiS.add(nonFreqBF);
                    }

                    for (List<String> nonFreqRec : nonFreqAttrs) {
                        HashSet<String> nonFreqQgrams = recAttrs2Qgrams.get(nonFreqRec);
                        if (new HashSet<>(nonFreqQgrams).containsAll(qi)) ViL.add(nonFreqRec);
                        if (new HashSet<>(qi).containsAll(nonFreqQgrams)) ViS.add(nonFreqRec);
                    }

                    if (ViS.size() <= m && ViS.size() > 0 && BiS.size() > 0 && BiS.size() <= m) {
                        Set<String> qu = new HashSet<>();
                        for (List<String> attrTemp : ViS) {
                            qu.addAll(recAttrs2Qgrams.get(attrTemp));
                        }
                        Set<String> qd = new HashSet<>(qi);
                        qd.removeAll(qu);
                        BitSet bo = new BitSet(l);
                        for (BitSet bj : BiS) bo.or(bj);
                        if (qd.size() > 0 && !bo.equals(bi)) {
                            for (int p = 0; p < l; p++) {
                                if (!bo.get(p) && bi.get(p)) {
                                    synchronized (CPr) {
                                        Set<String> set1 = CPr.getOrDefault(p, new HashSet<>());
                                        set1.addAll(qd);
                                        CPr.put(p, set1);
                                    }
                                    synchronized (CNr) {
                                        Set<String> set2 = CNr.getOrDefault(p, new HashSet<>());
                                        set2.addAll(qu);
                                        CNr.put(p, set2);
                                    }

                                    if (ViS.size() == 1 && BiS.size() == 1 && qd.size() == 1) {
                                        synchronized (CAr) {
                                            Set<String> set3 = CAr.getOrDefault(p, new HashSet<>());
                                            set3.addAll(qd);
                                            CAr.put(p, set3);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (ViL.size() <= m && ViL.size() > 0 && BiL.size() > 0 && BiL.size() <= m) {
                        Set<String> qu = new HashSet<>();
                        for (List<String> vj : ViL) {
                            qu.addAll(recAttrs2Qgrams.get(vj));
                        }
                        Set<String> qs = new HashSet<>();
                        int times = 0;
                        for (List<String> vj : ViL) {
                            if (times == 0) qs.addAll(recAttrs2Qgrams.get(vj));
                            else qs.retainAll(recAttrs2Qgrams.get(vj));
                            times++;
                        }
                        Set<String> qd = new HashSet<>(qs);
                        qi.forEach(qd::remove);
                        BitSet bo = new BitSet(l);
                        BitSet ba = new BitSet(l);
                        for (BitSet bfTemp : BiL) {
                            bo.or(bfTemp);
                            ba.and(bfTemp);
                        }
                        if (qd.size() > 0 && !ba.equals(bi)) {
                            for (int p = 0; p < l; p++) {
                                if (!bi.get(p) && ba.get(p)) {
                                    synchronized (CPr) {
                                        Set<String> set1 = CPr.getOrDefault(p, new HashSet<>());
                                        set1.addAll(qd);
                                        CPr.put(p, set1);
                                    }
                                    if (ViL.size() == 1 && BiL.size() == 1 && qd.size() == 1) {
                                        synchronized (CAr) {
                                            Set<String> set3 = CAr.getOrDefault(p, new HashSet<>());
                                            set3.addAll(qd);
                                            CAr.put(p, set3);
                                        }
                                    }
                                } else if (!bo.get(p) && !bi.get(p)) {
                                    synchronized (CNr) {
                                        Set<String> set2 = CNr.getOrDefault(p, new HashSet<>());
                                        set2.addAll(qu);
                                        CNr.put(p, set2);
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
                          Map<List<String>, HashSet<String>> recAttrs2Qgrams) {
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
                    HashSet<String> qgrams = recAttrs2Qgrams.get(recAttrs);
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
                    for (int pos = 0; pos < l; pos++) {
                        if (bfPositions.get(pos)) {
                            Set<String> qp = candiProbM.get(pos);
                            for (int p = 0; p < G.size(); p++) {
                                List<String> recAttrs = G.get(p);
                                List<String> qgrams = recAttrs2Qgrams.get(recAttrs);
                                int cnt = 0;
                                for (String qgram : qp) {
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
                        if (G.size() > 0) R.put(entityID, new HashSet<>(G));
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
                                                           Set<List<String>> GN, Map<List<String>, HashSet<String>> recAttrs2Qgrams) {
        Set<String> QN = new HashSet<>();
        for (int pos : candiNoProbM.keySet()) {
            QN.addAll(candiNoProbM.get(pos));
        }

        Map<String, BitSet> BQ = new HashMap<>();
        Map<List<String>, BitSet> BG = new HashMap<>();
        for (String qgram : QN) {
            BQ.put(qgram, new BitSet(l));
        }

        for (int pos = 0; pos < l; pos++) {
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
                    List<String> gj = GNList.get(j);
                    HashSet<String> qj = recAttrs2Qgrams.get(gj);
                    if (QN.containsAll(qj)) {
                        BitSet posTemp = BG.getOrDefault(gj, new BitSet(l));
                        posTemp.set(0, posTemp.size());
                        for (String qk : qj) {
                            posTemp.and(BQ.get(qk));
                        }
                        synchronized (BG) {
                            BG.put(gj, posTemp);
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
        for (String entityId : bfs.keySet()) {
            BitSet bi = bfs.get(entityId);
            Set<List<String>> Gi = new HashSet<>();
            for (List<String> gj : GN) {
                if (BG.containsKey(gj)) {
                    BitSet posTemp = BG.get(gj);
                    BitSet posTempTemp = posTemp.get(0, posTemp.size());
                    posTempTemp.and(bi);
                    if (posTempTemp.cardinality() == 0) Gi.add(gj);
                }
            }
            if (Gi.size() > 0) {
                R.put(entityId, Gi);
            }
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

        String attrFile = "D:/dataset/ncvoter/ncvoter-s-50000-1.csv";
        String storeNoProbFile = "D:/dataset/ncvoter/attack/attackNoProb_1.csv";
        String storeProbFile = "D:/dataset/ncvoter/attack/attackProb_1.csv";
        String bfFilePath = "D:/dataset/ncvoter/storeBF/";
        String storeFilePath = "D:/dataset/ncvoter/attack/";
        String datasetName = "s-1-1";
        String dateDiff = "none";
        int[] attr_index_list = {1};  // attr's columns used
        int entity_id_col = 0;

        EvalLinkage evalLinkage = new EvalLinkage(null, null, null);

        long startTime;
        long endTime;

        List<Map> result = evalLinkage.load_dataset_salt(attrFile, attr_index_list, entity_id_col, -1);
        Map<String, List<String>> rec_attr_val_dict = result.get(0);

        System.out.println("Generate qgrams for records");
        startTime = new Date().getTime();
        Map<List<String>, HashSet<String>> recAttr2Qgrams = evalAttack.getQgramsForRec(rec_attr_val_dict);
        endTime = new Date().getTime();
        System.out.printf(" generate qgrams for record cost time: %d msec\n", endTime - startTime);

        System.out.println("Sort record attributes");
        startTime = new Date().getTime();
        List<List<List<String>>> attrRes = evalAttack.sortAttrs(rec_attr_val_dict, freqT);
        List<List<String>> sortedUniqueFreqAttrs = attrRes.get(0);
        List<List<String>> sortedNonFreqAttrs = attrRes.get(1);

        endTime = new Date().getTime();
        System.out.printf(" freq records size: %d / non freq records size: %d\n", sortedUniqueFreqAttrs.size(), sortedNonFreqAttrs.size());
        System.out.printf(" sort record attributes cost time: %d msec\n", endTime - startTime);

        String[] hash_type_list = new String[]{"dh-false", "dh-true", "rh-false", "rh-true"};
        String[] num_hash_functions_list = {"opt"};
        String[] encode_type_list = {"clk"};
        String[] bf_harden_types = new String[]{
                "none",
                "salt",
                "bal",
                "rxor",
                "wxor-2",
                "r90",
                "xor",
                "blip-0.05",
                "blip-0.02",
                "urap-0.1-6",
                "urap-0.1-7",
                "urap-0.1-8",
                "urap-0.1-9",
                "urap-0.2-6",
                "urap-0.2-7",
                "urap-0.2-8",
                "urap-0.2-9",
                "urap-0.3-6",
                "urap-0.3-7",
                "urap-0.3-8",
                "urap-0.3-9",
                "urap-0.4-6",
                "urap-0.4-7",
                "urap-0.4-8",
                "urap-0.4-9",
                "urap-0.5-6",
                "urap-0.5-7",
                "urap-0.5-8",
                "urap-0.5-9",
                "urap-0.6-6",
                "urap-0.6-7",
                "urap-0.6-8",
                "urap-0.6-9",
                "urap-0.7-6",
                "urap-0.7-7",
                "urap-0.7-8",
                "urap-0.7-9",
                "urap-0.8-6",
                "urap-0.8-7",
                "urap-0.8-8",
                "urap-0.8-9",
                "urap-0.9-6",
                "urap-0.9-7",
                "urap-0.9-8",
                "urap-0.9-9",
                "indexd-0.05,0.02,0.01,0.02,0.05",
                "indexd-0.1,0.05,0.02,0.05,0.1",
                "indexd-0.15,0.1,0.05,0.1,0.15",
                "indexd-0.2,0.15,0.1,0.15,0.2",
                "indexd-0.2,0.1,0.05,0.02,0.01",
                "indexd-0.1,0.05,0.02,0.01,0.001",
                "indexd-0.08,0.04,0.02,0.01,0.005",
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
                        System.out.println("    Load bf file");
                        startTime = new Date().getTime();
                        bfs = evalAttack.loadBFs(bfFilePath, datasetName, encodeType, hashType, hardenType, numHashFunctions);
                        endTime = new Date().getTime();
                        System.out.printf("      load bf file cost time: %d msec\n", endTime - startTime);

                        System.out.println("    Sort Bloom Filters");
                        startTime = new Date().getTime();
                        List<List<BitSet>> bfsRes = evalAttack.sortBFs(bfs, freqT);
                        List<BitSet> sortedUniqueFreqBfs = bfsRes.get(0); // 频繁的bfs，且该频数唯一，倒序排序
                        List<BitSet> nonFreqBfs = bfsRes.get(1); // 不频繁的bfs
                        endTime = new Date().getTime();
                        System.out.printf("      unique freq bfs size: %d / non freq bfs size: %d\n", sortedUniqueFreqBfs.size(),
                                nonFreqBfs.size());
                        System.out.printf("      sort bfs cost time: %d msec\n", endTime - startTime);

                        System.out.println("    Generate Bloom Filter and Record Pair by freq");
                        Map<BitSet, List<String>> bfAttrPairs = new HashMap<>();
                        for (int i = 0; i < Math.min(sortedUniqueFreqBfs.size(), sortedUniqueFreqAttrs.size()); i++) {
                            BitSet bf = sortedUniqueFreqBfs.get(i);
                            bfAttrPairs.put(bf, sortedUniqueFreqAttrs.get(i));
                        }
                        System.out.printf("      bf and rec pairs size: %d msec\n", bfAttrPairs.size());

                        System.out.println("    Generate basic candidates(possible, no possible, assign)");
                        startTime = new Date().getTime();
                        Map<Integer, Set<String>> candiProbB = new HashMap<>();
                        Map<Integer, Set<String>> candiNoProbB = new HashMap<>();
                        evalAttack.getCandidateB(bfAttrPairs, candiProbB, candiNoProbB, recAttr2Qgrams);
                        Map<Integer, Set<String>> candiAssignB = new HashMap<>();
                        evalAttack.getQgramPosAssign(bfAttrPairs, candiProbB, candiAssignB, recAttr2Qgrams);
                        endTime = new Date().getTime();
                        System.out.printf("      generate basic candidates(possible, no possible, assign) cost time: %d msec\n",
                                endTime - startTime);

                        System.out.println("    Generate refined and expended candidates(possible, no possible, assign)");
                        startTime = new Date().getTime();
                        Map<Integer, Set<String>> candiProbR = new HashMap<>();
                        Map<Integer, Set<String>> candiNoProbR = new HashMap<>();
                        Map<Integer, Set<String>> candiAssignR = new HashMap<>();
                        evalAttack.qgramRefineAndExpend(bfAttrPairs, nonFreqBfs, sortedNonFreqAttrs, candiProbR,
                                candiNoProbR, candiAssignR, recAttr2Qgrams,
                                Math.max(bfs.size(), rec_attr_val_dict.size()));
                        endTime = new Date().getTime();
                        System.out.printf("      generate refined and expended candidates(possible, no possible, assign) cost time: %d msec\n",
                                endTime - startTime);

                        System.out.println("    Generate merged candidates(possible, no possible, assign)");
                        Map<Integer, Set<String>> candiProbM = new HashMap<>();
                        Map<Integer, Set<String>> candiNoProbM = new HashMap<>();
                        Map<Integer, Set<String>> candiAssignM = new HashMap<>();
                        for (int pos = 0; pos < evalAttack.l; pos++) {
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

//                        System.out.println("    Re-identify on possible candidates");
//                        startTime = new Date().getTime();
//                        Map<String, Set<List<String>>> ROnProb = evalAttack.identifyOnProb(bfs, candiProbM, GP, recAttr2Qgrams);
//                        endTime = new Date().getTime();
//                        System.out.printf("      re-identify on possible candidates cost time: %d msec\n", endTime - startTime);
//                        List<Integer> resultProb = evalAttack.statistic(rec_attr_val_dict, ROnProb);
//                        storeProbResult.put(method, resultProb);
//                        evalAttack.storeResult2File(storeFilePath + method + "-possible.csv", resultProb);
//                        System.out.printf("    Store result on possible in %s.csv\n", method);
//                        endTime = new Date().getTime();
//                        System.out.printf("    Re-identify on possible candidates result: 1-1:%d 1-m:%d w:%d\n",
//                                resultProb.get(0), resultProb.get(1), resultProb.get(2));
//                        System.out.printf("  Re-identify bf on possible encode:%s hash:%s harden:%s k:%s cost time: %d msec\n\n", encodeType, hashType,
//                                hardenType, numHashFunctions, endTime - startTime);


                        System.out.println("    Re-identify on no possible candidates");
                        startTime = new Date().getTime();
                        Map<String, Set<List<String>>> ROnNoProb = evalAttack.identifyOnNoProb(bfs, candiNoProbM, GN, recAttr2Qgrams);
                        endTime = new Date().getTime();
                        System.out.printf("      re-identify on no possible candidates cost time: %d msec\n", endTime - startTime);

                        String method = String.format("%s-encode%s-hash%s-harden%s-k%s-bfLen%d", datasetName,
                                encodeType, hashType, hardenType, numHashFunctions, evalAttack.l);
                        startTime = new Date().getTime();
                        List<Integer> resultNoProb = evalAttack.statistic(rec_attr_val_dict, ROnNoProb);
                        storeNoProbResult.put(method, resultNoProb);
                        evalAttack.storeResult2File(storeFilePath + method + "-nopossible.csv", resultNoProb);
                        System.out.printf("    Store result on no possible in %s.csv\n", method);
                        endTime = new Date().getTime();
                        System.out.printf("    Re-identify on no possible candidates result: 1-1:%d 1-m:%d w:%d\n",
                                resultNoProb.get(0), resultNoProb.get(1), resultNoProb.get(2));
                        System.out.printf("  Re-identify bf on no possible encode:%s hash:%s harden:%s k:%s cost time: %d msec\n\n", encodeType, hashType,
                                hardenType, numHashFunctions, endTime - startTime);
                    }
                }
            }
        }
//        evalAttack.storeResults2File(storeProbFile, storeProbResult);
//        System.out.printf("Store result on possible candidates in %s, result size: %d\n", storeProbFile, storeProbResult.size());
        evalAttack.storeResults2File(storeNoProbFile, storeNoProbResult);
        System.out.printf("Store result on no possible candidates in %s, result size: %d\n", storeNoProbFile, storeNoProbResult.size());

        evalAttack.threadPool.shutdown();
    }

    public List<Integer> statistic(Map<String, List<String>> recAttrsDict, Map<String, Set<List<String>>> R) {
        int one2one = 0;
        int one2many = 0;
        int wrong = 0;
        for (String entityID : recAttrsDict.keySet()) {
            List<String> recAttrs = recAttrsDict.get(entityID);
            int cnt = 0;
            if (R.containsKey(entityID)) {
                for (List<String> recAttrsG : R.get(entityID)) {
                    if (recAttrs.equals(recAttrsG)) {
                        cnt += 1;
                    }
                }
                if (cnt > 0) {
                    if (cnt == R.get(entityID).size()) one2one += 1;
                    else one2many += 1;
                } else wrong += 1;
            } else wrong += 1;
        }
        return Arrays.asList(one2one, one2many, wrong);
    }

    private Map<List<String>, HashSet<String>> getQgramsForRec(Map<String, List<String>> rec_attr_val_dict) {
        Map<List<String>, HashSet<String>> recAttrs2Qgrams = new HashMap<>();
        for (List<String> recAttrs : rec_attr_val_dict.values()) {
            List<String> qgrams = qGramListForRec(recAttrs);
            recAttrs2Qgrams.put(recAttrs, new HashSet<>(qgrams));
        }
        return recAttrs2Qgrams;
    }

    private void storeResult2File(String storeFile, List<Integer> storeResult) {
        File file = new File(storeFile);
        try (FileWriter fw = new FileWriter(file, false);
             BufferedWriter bw = new BufferedWriter(fw)) {
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    System.out.printf("创建文件%s失败\n", storeFile);
                    return;
                }
            }

            String line = "one2one,one2many,wrong";
            bw.write(line + "\n");
            bw.write(String.format("%d,%d,%d\n", storeResult.get(0), storeResult.get(1), storeResult.get(2)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void storeResults2File(String storeFile, Map<String, List<Integer>> storeResult) {
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
