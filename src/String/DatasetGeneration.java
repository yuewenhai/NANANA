package String;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatasetGeneration {
    List<List<String>> getDataset(String datasetName, int datasetSize){
        List<List<String>> dataset = new ArrayList<>();
        String datasetFile = "dataset/" + datasetName;

        File file = new File(datasetFile);
        if (!file.exists()) {
            System.out.printf("数据集: %s 不存在%n", dataset);
        }else {
            try (FileReader fr = new FileReader(file);
                 BufferedReader br = new BufferedReader(fr)) {
                String line = br.readLine();
                int count = 0;
                while (line != null && count++ < datasetSize){
                    List<String> lineArray = Arrays.stream(line.replace("\"", "").split(",")).toList();
                    dataset.add(lineArray);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return dataset;
    }
}
