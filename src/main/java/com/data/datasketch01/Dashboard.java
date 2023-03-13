package com.data.datasketch01;

// Common
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.io.File;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.functions.runtime.shaded.org.apache.zookeeper.server.WorkerService.WorkRequest;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Arrays;
import java.text.NumberFormat;
import java.util.Locale;


public class Dashboard {

    public static void clearReportFolder() throws FileNotFoundException, IOException, Exception {
           File directory = new File(App.reportpath);
           File[] files = directory.listFiles();
           for (File file:files) {
               file.delete();
           } 
        System.out.println("Reports folder purged.");
        }
    
    public static long getFileSize(String filename) throws FileNotFoundException, IOException, Exception {
        File f = new File("bins/"+filename);
        long fileSize = FileUtils.sizeOf(f);
        
        /* String format = "%-40s%s%n";        
        System.out.printf(format,"The size of the file " + filename + " in bytes: ", fileSize); */

        Path copied = Paths.get(App.reportpath+filename);
        Path originalPath = Paths.get(App.workpath+filename);
        Files.copy(originalPath, copied, StandardCopyOption.REPLACE_EXISTING);
        
        f.delete();
        return fileSize;
    }

    public static void dashboard() throws FileNotFoundException, IOException, Exception {
        // -- Set up report file
        File file = new File(App.reportfile);
        PrintStream stream = new PrintStream(file);
        System.setOut(stream);
        // -- Frequency Report
        System.out.println(App.manyLines);
        System.out.printf("%-25s %-25s %-15s %n", "Result Type", "Result Value", "Accuracy/Count");
        System.out.println(App.manyLines);
        Map<String, String> displayMap = App.sketchTable.row("Result:");
        for (Map.Entry<String, String> tableInfo : displayMap.entrySet()) {
            String str = tableInfo.getValue();
            String strNew = str.replace(",","");
            double str1 = Double.parseDouble(strNew);
            Double accuracy = (str1 / App.amount);
            String a = tableInfo.getKey();
            String b = tableInfo.getValue(); 
            String c = NumberFormat.getNumberInstance(Locale.US).format(accuracy);
            System.out.printf("%-25s %-25s %-15s %n", a,b,c);
        }

        Map<String, String> dataMap = App.dataTable.row("Result:");
        for (Map.Entry<String, String> sketchInfo : dataMap.entrySet()) {
            String a2 = sketchInfo.getKey();
            String b2 = "Result: " + sketchInfo.getValue(); 
            String c2 = "-";
            System.out.printf("%-25s %-25s %-15s %n", a2,b2,c2);
        }

        Map<String, String> mostFreq = App.MostFreqTable.row("Result:");
        for (Map.Entry<String, String> MFInfo : mostFreq.entrySet()) {
            String a2 = "Most Freq";
            String b2 = "Value: " + MFInfo.getKey();
            String c2 = "Result: " + MFInfo.getValue(); 
            
            System.out.printf("%-25s %-25s %-15s %n", a2,b2,c2);
        }

        // -- Variable Information
        System.out.println(App.manyLines);
        System.out.printf("%-16s %s %n","Variables:","Settings: ");
        System.out.println(App.manyLines);
        System.out.println("Integers: \t " + App.numbers);
        System.out.println("Strings : \t " + App.strings);
        System.out.println("IPs     : \t " + App.ips);
        System.out.println("Amount  : \t " + NumberFormat.getNumberInstance(Locale.US).format(App.amount));
        System.out.println("lgk CPC : \t " + App.ClgK);
        System.out.println("lgk HLL : \t " + App.HlgK);

        // -- File & Size Information
        System.out.println(App.manyLines);
        String format = "%-25s%s%n"; 
        System.out.printf(format,"File Names:","Bytes:");
        System.out.println(App.manyLines);
        File f = new File(App.workpath);
        String [] dirListing = f.list();
        Arrays.sort(dirListing);
        for (String pathname : dirListing) {       
            System.out.printf(format,pathname, NumberFormat.getNumberInstance(Locale.US).format(getFileSize(pathname)));               
        }
        


        // -- Sketch Stopwatch
        System.out.println(App.manyLines);
        System.out.printf("%-25s %-35s %n","Sketch:","Time: ");
        System.out.println(App.manyLines);
        
        System.out.printf("%-25s %-35s %n", "Frequency", "Value");
        Map<String, String> timerMap = App.timerTable.row("Time");
        for (Map.Entry<String, String> timerInfo : timerMap.entrySet()) {
            String a3 = timerInfo.getKey();
            String b3 = timerInfo.getValue(); 
            System.out.printf("%-25s %-35s %n", a3,b3);
        }
    }
}
