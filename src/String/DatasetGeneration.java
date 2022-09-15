package String;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DatasetGeneration {
    public List<List<String>> getDataset(String datasetName, String filename, int datasetSize, String[] attributes){
        List<List<String>> dataset = new ArrayList<>();
        String datasetFileFolder = "dataset/" + datasetName;
        String datasetFile = datasetFileFolder + "/" + filename;
        File file = new File(datasetFile);
        if (!file.exists()) {
            System.out.printf("数据集: %s 不存在%n", datasetFile);
        }else {
            try (FileReader fr = new FileReader(file);
                 BufferedReader br = new BufferedReader(fr)) {
                String[] datasetColumns = br.readLine().replace("\"", "").split(",");
                int[] indexes = new int[attributes.length];
                for (int i = 0;i < attributes.length;i++){
                    for (int j = 0;j < datasetColumns.length;j++){
                        if (datasetColumns[j].equals(attributes[i]))
                            indexes[i] = j;
                    }
                }
                int count = 0;
                String line = br.readLine();
                String[] lineArray;
                boolean skip = false;
                while (line != null && count++ < datasetSize){
                    List<String> dataLine = new ArrayList<>();
                    lineArray = line.replace("\"", "").split(",");
                    for (int index : indexes) {
                        if (lineArray[index].equals("") || lineArray[index] == null) skip = true;
                        dataLine.add(lineArray[index]);
                    }
                    if (skip) {
                        skip = false;
                        line = br.readLine();
                        continue;
                    }
                    dataset.add(dataLine);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return dataset;
    }
}
