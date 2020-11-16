import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) {
        File catalog = new File("Test//in");
        List<List<String>> result = new ArrayList<List<String>>();
        List<List<String>> out = new ArrayList<List<String>>();
        List<Future<List<List<String>>>> list = new ArrayList<Future<List<List<String>>>>();
        Callable<List<List<String>>> callable;
        ExecutorService executor = null;

        try {
            executor = Executors.newFixedThreadPool(catalog.listFiles().length);
            for(File cFile:catalog.listFiles()){
                callable = new AThread(cFile.getAbsolutePath());
                Future<List<List<String>>> future = executor.submit(callable);
                list.add(future);
            }
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }

        for(Future<List<List<String>>> future : list){
            try {
                for (List<String> rList:future.get()){
                    result.add(rList);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();

        Map<String,List<String>> resMap = new HashMap<>();
        List<String> values;
        Set<String> uniValues;

        for (List<String> tlist:result){
           if (!resMap.containsKey(tlist.get(0))){
               uniValues = new LinkedHashSet<>(tlist.subList(1,tlist.size()));
               values = new ArrayList<>(uniValues);
               resMap.put(tlist.get(0),values);
           }else {
               values = new ArrayList<>();
               values.addAll(resMap.get(tlist.get(0)));
               values.addAll(tlist.subList(1,tlist.size()));
               uniValues = new LinkedHashSet<>(values);
               values = new ArrayList<>(uniValues);
               resMap.put(tlist.get(0),values);
           }
        }

        for (Map.Entry<String,List<String>> entry:resMap.entrySet()){
            try(FileWriter writer = new FileWriter("Test//out//"+entry.getKey() + ".csv",false)) {
                writer.write(entry.getKey() + ":");
                writer.append('\n');
                for (String value:entry.getValue()){
                    writer.write(value + ";");
                }
            writer.flush();

            }catch (IOException e){
                System.out.println(e.getMessage());
            }
        }

        for (Map.Entry<String,List<String>> entry:resMap.entrySet()){
            System.out.println(entry.getKey() + ":");
            for (String value:entry.getValue()){
                System.out.print(value + ";");
            }
            System.out.println();
        }
    }
}

    class AThread implements Callable<List<List<String>>> {
    private String path;
    AThread(String path){
        this.path = path;
    }

    public List<List<String>> call() throws Exception {
        BufferedReader br;
        String cvsSplitBy = ";";
        String line;

        List<List<String>> inputList = new ArrayList<>();

        try {
            br = new BufferedReader(new FileReader(path));
            while ((line = br.readLine()) != null) {
                inputList.add(Arrays.asList(line.split(cvsSplitBy)));

            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }

        String[][]rotatedList = new String[inputList.get(0).size()][inputList.size()];

        for (int i = 0; i < inputList.size(); i++ ){
            for (int j = 0; j < inputList.get(i).size(); j++){
                rotatedList[inputList.get(i).size() - 1 - j][i] = inputList.get(i).get(j);
            }
        }

        List<List<String>> lists = new ArrayList<>();

        for (int i = 0;i<= rotatedList.length-1;i++){
            lists.add(Arrays.asList(rotatedList[i]));
        }
        return lists;
    }
}
