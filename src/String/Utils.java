package String;

import java.util.*;

public class Utils {
    public static Map<String, Map> splitRecAttrValByType(Map<String, List<String>> rec_attr_val_dict, String[] attrTypeList) {
        Map<String, List<String>> recStrAttrDict = new HashMap<>();
        Map<String, List<Integer>> recIntAttrDict = new HashMap<>();
        Map<String, List<String>> recLocAttrDict = new HashMap<>();

        for (String entityID : rec_attr_val_dict.keySet()) {
            List<String> attrList = rec_attr_val_dict.get(entityID);
            for (int i = 0; i < attrList.size(); i++) {
                switch (attrTypeList[i]) {
                    case "str" -> {
                        List<String> strAttrList = recStrAttrDict.getOrDefault(entityID, new ArrayList<>());
                        strAttrList.add(attrList.get(i));
                        recStrAttrDict.put(entityID, strAttrList);
                    }
                    case "int" -> {
                        List<Integer> intAttrList = recIntAttrDict.getOrDefault(entityID, new ArrayList<>());
                        intAttrList.add(Integer.parseInt(attrList.get(i)));
                        recIntAttrDict.put(entityID, intAttrList);
                    }
                    case "loc" -> {
                        List<String> locAttrList = recLocAttrDict.getOrDefault(entityID, new ArrayList<>());
                        locAttrList.add(attrList.get(i));
                        recLocAttrDict.put(entityID, locAttrList);
                    }
                }
            }
        }

        Map<String, Map> res = new HashMap<>();
        res.put("str", recStrAttrDict);
        res.put("int", recIntAttrDict);
        res.put("loc", recLocAttrDict);
        return res;
    }

    public static Map<Double, int[]> calc_linkage_outcomes(Map<String[], Double> rec_pair_dict, double[] sim_threshold_list) {
        Map<Double, int[]> class_res_dict = new HashMap<>();

        for (double sim_threshold : sim_threshold_list) {
            class_res_dict.put(sim_threshold, new int[]{0, 0, 0, 0});  // Initalise the results counters
        }
        boolean is_true_match;
        for (String[] entity_id_pair : rec_pair_dict.keySet()) {
            is_true_match = entity_id_pair[0].equals(entity_id_pair[1]);

            //  Calculate linkage results for all given similarities
            //
            double sim = rec_pair_dict.get(entity_id_pair);
            for (double sim_threshold : sim_threshold_list) {
                int[] sim_res_list = class_res_dict.get(sim_threshold);

                if (sim >= sim_threshold && is_true_match)  // TP
                    sim_res_list[0] += 1;
                else if (sim >= sim_threshold && !is_true_match) // FP
                    sim_res_list[1] += 1;
                else if (sim < sim_threshold && is_true_match)  // FN
                    sim_res_list[3] += 1;
                // TN are calculated at the end
            }
        }
        for (double sim_threshold : sim_threshold_list) {
            int[] sim_res_list = class_res_dict.get(sim_threshold);

            // Calculate the number of TN as all comparisons - (TP + FP + FN)
            //
            sim_res_list[2] = rec_pair_dict.keySet().size() - Arrays.stream(sim_res_list).sum();
        }
        return class_res_dict;
    }

    public static double calc_precision(int num_tp, int num_fp) {
        double precision;
        if ((num_tp + num_fp) > 0)
            precision = (double) num_tp / (num_tp + num_fp);
        else precision = 0.0;

        return Double.parseDouble(String.format("%.4f", precision));
    }

    public static double calc_recall(int num_tp, int num_fn) {
        double recall;
        if ((num_tp + num_fn) > 0)
            recall = (double) num_tp / (num_tp + num_fn);
        else recall = 0.0;

        return Double.parseDouble(String.format("%.4f", recall));
    }


    public static void calc_precisions_recalls(double[] sim_threshold_list, Map<Double, int[]> class_res_dict, List<Double> precisions, List<Double> recalls) {
        int[] tp_fp_tn_fn;
        int tp;
        int fp;
        int fn;
        double precision;
        double recall;
        for (double sim_threshold : sim_threshold_list) {
            tp_fp_tn_fn = class_res_dict.get(sim_threshold);
            tp = tp_fp_tn_fn[0];
            fp = tp_fp_tn_fn[1];
            fn = tp_fp_tn_fn[3];
            precision = calc_precision(tp, fp);
            precisions.add(precision);
            recall = calc_recall(tp, fn);
            recalls.add(recall);
        }
    }
}
