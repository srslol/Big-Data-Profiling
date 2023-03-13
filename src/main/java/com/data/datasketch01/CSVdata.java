package com.data.datasketch01;

import java.text.NumberFormat;
import java.util.Locale;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.Arrays;
import com.opencsv.*;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Intersection;



public class CSVdata {

    public static void readCSVbyLine(String file) throws IOException {
        
        ArrayList<String> cell_list = new ArrayList<String>();
        String[] nextRecord;


        try {
 
            // Create an object of filereader
            // class with CSV file as a parameter.
            FileReader filereader = new FileReader(file);
     
            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader);
            
            // we are going to read data line by line
            while ((nextRecord = csvReader.readNext()) != null) {
                for (String cell : nextRecord) {
                    //System.out.print(cell + "\t");
                    cell_list.add(cell.toString());
                   
                }
                System.out.println();
            }
            csvReader.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        /* for (String position : cell_list) {
            System.out.println("Value = " + position);
         }   */
        
        CpcSketch csv_list = new CpcSketch(App.ClgK);
        for (String items : cell_list) {
            String value = ((String)items);  
            csv_list.update(value);
            
            }
        FileOutputStream out1 = new FileOutputStream(App.csv_cpc);
        out1.write(csv_list.toByteArray());
        out1.close();
        

        HllSketch hll_list = new HllSketch(App.HlgK);
        for (String items : cell_list) {
            String value = ((String)items);  
            hll_list.update(value);
            
            }
        FileOutputStream out4 = new FileOutputStream(App.csv_hll);
        out4.write(hll_list.toCompactByteArray());
        out4.close();


        UpdateSketch theta_list = UpdateSketch.builder().build();
        for (String items : cell_list) {
            String value = ((String)items);  
            theta_list.update(value);
            
            }
        FileOutputStream out7 = new FileOutputStream(App.csv_theta);
        out7.write(theta_list.compact().toByteArray());
        out7.close();
        



        // //////////// Pull back in 
        FileInputStream in1 = new FileInputStream(App.csv_cpc);
        byte[] bytes1 = new byte[in1.available()];
        in1.read(bytes1);
        in1.close();
        CpcSketch csv_cpc_sketch = CpcSketch.heapify(Memory.wrap(bytes1));
        
   
        FileInputStream in2 = new FileInputStream(App.csv_hll);
        byte[] bytes2 = new byte[in2.available()];
        in2.read(bytes2);
        in2.close();
        HllSketch csv_hll_sketch = HllSketch.heapify(Memory.wrap(bytes2));


        FileInputStream in3 = new FileInputStream(App.csv_theta);
        byte[] bytes3 = new byte[in3.available()];
        in3.read(bytes3);
        in3.close(); 
        Sketch csv_theta_sketch = Sketches.wrapSketch(Memory.wrap(bytes3));

        



        System.out.println("CSV CPC      :" + csv_cpc_sketch.getEstimate());
        System.out.println("CSV HLL      :" + csv_hll_sketch.getEstimate());
        System.out.println("CSV THETA    :" + csv_theta_sketch.getEstimate());
      
        System.out.println("CSV Length   :" + cell_list.size());
        // System.out.println(csv_sketch1.toString());

    }
}
