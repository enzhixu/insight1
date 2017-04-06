import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by enzhi on 4/5/2017.
 */
public class WebData8 {
    static int BUFFER_SIZE = 10000000;          //Will use 10000000
    static int PQ_SIZE = 10000000;              //Will use 10000000
    static int K = 10;

    static Duration ONE_MINUTE = Duration.ofMinutes(1);
    static Duration WINDOW = Duration.ofSeconds(20);
    static Duration BLOCK_PERIOD = Duration.ofMinutes(5);
    static DateTimeFormatter formatter;

    static List<String> addresses;
    static List<ZonedDateTime> zonedDateTimes;
    static List<String> resources;
    static List<String> httpResults;
    static List<Integer> resultSizes;
    static PriorityQueue<Map.Entry<String, Long>> topAddress;
    static PriorityQueue<Map.Entry<String, Long>> topResource;
    static PriorityQueue<Map.Entry<ZonedDateTime, Long>> topPeriods;
    static List<String> blockedList;
    static Map<String, Deque<ZonedDateTime>> failMap;

    static String inputFile;
    static String hostFile;
    static String hoursFile;
    static String resourcesFile;
    static String blockFile;

    public static void main(String[] args) {
        //String file = "src/../log_input/log.txt";
        inputFile = args[0];
        hostFile = args[1];
        hoursFile = args[2];
        resourcesFile = args[3];
        blockFile = args[4];
        analyzeData(inputFile);
    }

    static void analyzeData (String file) {
        ZonedDateTime startTime = ZonedDateTime.now();

        formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
        addresses = new ArrayList<>();
        zonedDateTimes = new ArrayList<>();
        resources = new ArrayList<>();
        httpResults = new ArrayList<>();
        resultSizes = new ArrayList<>();
        topAddress = new PriorityQueue<>();
        topResource = new PriorityQueue<>();
        topPeriods = new PriorityQueue<>();
        blockedList = new ArrayList<>();
        failMap = new HashMap<>();

        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            long lineNumber = 1L;
            int remaining = 0;
            //Clear the old file
            outputToFile(new ArrayList<String>(), blockFile, false);

            for(String line; (line = br.readLine()) != null; ) {
//                if (lineNumber > 1000000){
//                    break;
//                }
                try {
                    parseData(line, lineNumber);
                } catch (Exception e) {
                    System.out.println("Exception at line " + lineNumber);
                }
                if (lineNumber % BUFFER_SIZE == 0) {        //Analyze data every BUFFER_SIZE lines
                    remaining = update(remaining);
                }
                lineNumber++;
            }
            lineNumber--;
            System.out.println("address size: " + addresses.size());
            update(remaining);
            printResult();

            ZonedDateTime endTime = ZonedDateTime.now();
            System.out.println("Total line number: " + lineNumber);
            System.out.println("Total running time: " + Duration.between(startTime, endTime));

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    //Covert a line of raw data into formatted data
    static void parseData (String line, long lineNumber) throws Exception{
        String[] strs = line.split(" ");
        String address = strs[0];
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(strs[3].substring(1) + " " + strs[4].substring(0, 5), formatter);
        String resource = strs[6];
        String httpResult = strs[strs.length - 2];
        Integer resultSize;
        try {
            resultSize = Integer.parseInt(strs[strs.length - 1]);
        } catch (Exception e) {
            resultSize = 0;
        }
        addresses.add(address);
        zonedDateTimes.add(zonedDateTime);
        resources.add(resource);
        httpResults.add(httpResult);
        resultSizes.add(resultSize);
        if (resource.equals("/login")) {
            //If this line needs to be blocked, add it to blockedList
            blockFailueSingle (line, address, httpResult, zonedDateTime, lineNumber);
        }

    }

    //print and save results
    static void printResult () {
        while (topAddress.size() > K) {
            topAddress.poll();
        }
        List<String> addressRes = new ArrayList<>();
        while (!topAddress.isEmpty()) {
            addressRes.add(topAddress.peek().getKey() + "," + topAddress.peek().getValue() + "\n");
            System.out.println(topAddress.poll());
        }
        Collections.reverse(addressRes);
        outputToFile(addressRes, hostFile, false);

        while (topResource.size() > K) {
            topResource.poll();
        }
        List<String> resourceRes = new ArrayList<>();
        while (!topResource.isEmpty()) {
            resourceRes.add(topResource.peek().getKey() + ", " + topResource.peek().getValue() + "bytes\n");
            System.out.println(topResource.poll());
        }
        Collections.reverse(resourceRes);
        outputToFile(resourceRes, resourcesFile, false);


//        while (topPeriods.size() > K) {
//            topPeriods.poll();
//        }

        List<Map.Entry<ZonedDateTime, Long>> periodList = new ArrayList<>();
        while (!topPeriods.isEmpty()) {
            periodList.add(topPeriods.poll());
        }
        Collections.sort(periodList, new Comparator<Map.Entry<ZonedDateTime, Long>>() {
            @Override
            public int compare(Map.Entry<ZonedDateTime, Long> o1, Map.Entry<ZonedDateTime, Long> o2) {
                if (o1.getValue() < o2.getValue()) {
                    return 1;
                } else if (o1.getValue() > o2.getValue()) {
                    return -1;
                } else {
                    return o1.getKey().compareTo(o2.getKey());
                }
            }
        });
        List<String> periodRes = new ArrayList<>();
        for (int i = 0; i < periodList.size() && i < K; i++) {
            periodRes.add(formatter.format(periodList.get(i).getKey()) + "," + periodList.get(i).getValue() + "\n");
            System.out.print(periodRes.get(periodRes.size() - 1));
        }
        outputToFile(periodRes, hoursFile, false);
        for (int i = 0; i < 10 && i < blockedList.size(); i++) {
            System.out.print(blockedList.get(i));
        }
    }

    //Write a list of String into a file
    static void outputToFile (List<String> content, String fileName, boolean append) {
        try {
            FileWriter writer;
            if (append) {
                writer = new FileWriter(fileName, true);
            } else {
                writer = new FileWriter(fileName);
            }
            for(String str: content) {
                writer.write(str);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Process, save data, and clear old data. Return the size of the remaining data
    static int update(int offset) {
        topAddress = getTopK(offset, topAddress, addresses, PQ_SIZE, null);
        topResource = getTopK(offset, topResource, resources, PQ_SIZE, resultSizes);
        topPeriods = getTopKPeriods(topPeriods, zonedDateTimes, PQ_SIZE);
        outputToFile(blockedList, blockFile, true);
        blockedList.clear();
        int n = zonedDateTimes.size();
        ZonedDateTime lastDateTime = zonedDateTimes.get(n - 1);
        int remaining = n;
        for (int i = n - 1; i >= 0; i--) {
            if (Duration.between(zonedDateTimes.get(i), lastDateTime).compareTo(ONE_MINUTE) > 0) {
                remaining = n - i - 1;
                break;
            }
        }
        //clear old data, leaving the data within the last 1 min
        zonedDateTimes.subList(0, n - remaining).clear();
        addresses.subList(0, n - remaining).clear();
        resources.subList(0, n - remaining).clear();
        httpResults.subList(0, n - remaining).clear();
        return remaining;   //Size of the remaining data
    }

    //Return the top k entries in a map whose values are largest
    static <T> PriorityQueue<Map.Entry<T, Long>> getTopKMap (Map<T, Long> map, int k) {
        PriorityQueue<Map.Entry<T, Long>> pq = new PriorityQueue<>(((o1, o2) -> o1.getValue().compareTo(o2.getValue())));
        for (Map.Entry<T, Long> entry : map.entrySet()) {
            pq.offer(entry);
            if (pq.size() > k) {
                pq.poll();
            }
        }
        return pq;
    }

    //Return the top k most frequent (with or without weight) element from list, merge to the previous results
    static PriorityQueue<Map.Entry<String, Long>> getTopK
    (int offset, PriorityQueue<Map.Entry<String, Long>> previous, List<String> list, int k, List<Integer> weight) {
        Map<String, Long> map = new HashMap<>();       //Key: element, Value: frequency or weighted frequency
        if (weight == null) {
            for (int i = offset; i < list.size(); i++) {
                map.put(list.get(i), map.getOrDefault(list.get(i), 0L) + 1);
            }
        } else {
            for (int i = offset; i < list.size(); i++) {
                map.put(list.get(i), map.getOrDefault(list.get(i), 0L) + weight.get(i));
            }
        }
        //Load the previous results into map
        while (!previous.isEmpty()) {
            Map.Entry<String, Long> entry = previous.poll();
            map.put(entry.getKey(), map.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
        PriorityQueue<Map.Entry<String, Long>> pq = getTopKMap(map, k);
        return pq;
    }
    //Return the top k most frequent element from dates, merge to the previous results
    static PriorityQueue<Map.Entry<ZonedDateTime, Long>> getTopKPeriods
    (PriorityQueue<Map.Entry<ZonedDateTime, Long>> previous, List<ZonedDateTime> dateTimes, int k) {
        Map<ZonedDateTime, Long> map = new HashMap<>();     //Key: start time, Value: frequency
        int start = 0, end = 0, n = dateTimes.size();
        //int n = dateTimes.size();
        //System.out.println(Duration.between(dateTimes.get(start), dateTimes.get(5)).compareTo(ONE_MINUTE) <= 0);
        for (start = 0; start < n; start++) {
            ZonedDateTime startTime = dateTimes.get(start);
            ZonedDateTime nextStart = start < n - 1 ? dateTimes.get(start + 1) : startTime.plusSeconds(1);
            while (startTime.compareTo(nextStart) < 0) {
                while (end < n - 1 && Duration.between(startTime, dateTimes.get(end + 1)).compareTo(ONE_MINUTE) <= 0) {
                    end++;
                }
                int adjust = startTime.equals(dateTimes.get(start)) ? 1 : 0;
                map.put(startTime, (long) end - start + adjust);
                startTime = startTime.plusSeconds(1);
            }
        }

        System.out.println("map size: " + map.size());
        //Load the previous results into map
        while (!previous.isEmpty()) {
            Map.Entry<ZonedDateTime, Long> entry = previous.poll();
            if (!map.containsKey(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        PriorityQueue<Map.Entry<ZonedDateTime, Long>> pq = getTopKMap(map, k);
        return pq;
    }

    //If the current line should be blocked, add it to blockedList
    static void blockFailueSingle (String line, String address, String httpResult, ZonedDateTime zonedDateTime, long lineNumber) {
        //Loop over the map and delete entried earlier than 5 min ago
        for(Iterator<Map.Entry<String, Deque<ZonedDateTime>>> iter = failMap.entrySet().iterator(); iter.hasNext(); ) {
            Deque<ZonedDateTime> dateTimeList = iter.next().getValue();
            if (Duration.between(dateTimeList.peekLast(), zonedDateTime).compareTo(BLOCK_PERIOD) > 0)  {
                iter.remove();
            }
        }

        if (!failMap.containsKey(address)) {
            if (httpResult.charAt(0) == '2') {
                return;
            } else {
                //Create a failed login counter
                Deque<ZonedDateTime> deque = new ArrayDeque<>();
                deque.offerLast(zonedDateTime);
                failMap.put(address, deque);
            }
        } else {
            Deque<ZonedDateTime> deque = failMap.get(address);
            if (deque.size() == 3) {
                //needs to block the current line
                blockedList.add(line + "\n");
            } else if (httpResult.charAt(0) == '2') {
                //Successful login resets previous failed login counter
                failMap.remove(address);
            } else {
                //As long as first element is earlier than 20 s ago, delete it
                while (!deque.isEmpty() && Duration.between(deque.peekFirst(), zonedDateTime).compareTo(WINDOW) > 0) {
                    deque.pollFirst();
                }
                deque.offerLast(zonedDateTime);
            }
        }
    }
}
