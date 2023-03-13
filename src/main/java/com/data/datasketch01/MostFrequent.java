package com.data.datasketch01;

// Common
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import com.google.common.base.Stopwatch;
import java.util.HashMap;
import java.util.Map;

// Frequent Items Sketch
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.LongsSketch.Row;


public class MostFrequent {

    public static void frequencyCreateBINS() throws FileNotFoundException, IOException {
        Generate.mapWords();
        
        ItemsSketch<String> sketchf1 = new ItemsSketch<String>(64);
        for (Map.Entry mapElementintegers : App.hash_map.entrySet()) { //word1_map
            String value = (String)mapElementintegers.getValue();
            sketchf1.update(value.toString());  
            }
        FileOutputStream out1 = new FileOutputStream(App.freqstring1);
        out1.write(sketchf1.toByteArray(new ArrayOfStringsSerDe()));
        out1.close();

        ItemsSketch<String> sketchf2 = new ItemsSketch<String>(64);
        for (Map.Entry mapElementintegers : App.hash_map.entrySet()) {
            String value = (String)mapElementintegers.getValue();
            sketchf2.update(value.toString());  
            }
        FileOutputStream out2 = new FileOutputStream(App.freqstring2);
        out2.write(sketchf2.toByteArray(new ArrayOfStringsSerDe()));
        out2.close();

        ItemsSketch<String> sketchf3 = new ItemsSketch<String>(64);
        for (int a=1;a<App.amount;a++) {
            String randomIP = Generate.createRandomIP(2);   //  int represents max octect value (2 = 2.2.2.2 max)
            sketchf3.update(randomIP);
        }   
        FileOutputStream out3 = new FileOutputStream(App.freqstring3);
        out3.write(sketchf3.toByteArray(new ArrayOfStringsSerDe()));
        out3.close();

    }

    public static void frequencySketchCreate() throws FileNotFoundException, IOException  {
        Stopwatch stopwatch = Stopwatch.createStarted();
        ItemsSketch<String> qunion = new ItemsSketch<String>(64);
       
        FileInputStream in1 = new FileInputStream(App.freqstring1);
        byte[] qbytes1 = new byte[in1.available()];
        in1.read(qbytes1);
        in1.close();
        ItemsSketch<String> sketch1 = ItemsSketch.getInstance(Memory.wrap(qbytes1), new ArrayOfStringsSerDe());
        qunion.merge(sketch1);
        

        FileInputStream in2 = new FileInputStream(App.freqstring2);
        byte[] qbytes2 = new byte[in2.available()];
        in2.read(qbytes2);
        in2.close();
        ItemsSketch<String> sketch2 = ItemsSketch.getInstance(Memory.wrap(qbytes2), new ArrayOfStringsSerDe());
        qunion.merge(sketch2);

        FileInputStream in3 = new FileInputStream(App.freqstring3);
        byte[] qbytes3 = new byte[in3.available()];
        in3.read(qbytes3);
        in3.close();
        ItemsSketch<String> sketch3 = ItemsSketch.getInstance(Memory.wrap(qbytes3), new ArrayOfStringsSerDe());
        qunion.merge(sketch3);
        
// ************************** review with better data ********************************
        ItemsSketch.Row<String>[] items = qunion.getFrequentItems(ErrorType.NO_FALSE_POSITIVES); 
        // System.out.println("Frequent items: " + items.length);
        // System.out.println(ItemsSketch.Row.getRowHeader());
        for (ItemsSketch.Row<String> row: items) {
            
            String x = String.valueOf(row.getEstimate());  //getUpper
            String y = String.valueOf(row.getItem());
            App.MostFreqTable.put("Result:", x , y);
                        
        }

        
        /* for (ItemsSketch.Row<String> row: items) {
            System.out.println(row.getEstimate() + "\t\t" + row.getItem());
        } */
      /*   System.out.println("---- union ---> " + qunion.getFrequentItems(ErrorType.NO_FALSE_POSITIVES).toString());
        for (ItemsSketch.Row<String> row: items) {
            // System.out.println(row.getEstimate() + "\t\t" + row.getItem());
            Long estimate = row.getEstimate();

            App.sketchTable.put("Result", estimate.toString(), row.getItem().toString());
            

        } */
        // System.out.println("------freq---> " + items.length);

        App.timerTable.put("Time", "Frequency", stopwatch.toString());
        stopwatch.reset();
    }
}
