package com.data.datasketch01;
// Module to generate simulated data


// Common Imports
import java.text.NumberFormat;
import java.util.Locale;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.validator.routines.InetAddressValidator;



// CPC Imports
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion; 

// HLL Imports
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType; 

// Theta Sketch Import
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;

// Tuple Sketch Import
import org.apache.datasketches.tuple.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;


public class Generate {

    public static String formatNumber(Integer convert) {
        String formatted = NumberFormat.getNumberInstance(Locale.US).format(convert);
        return formatted;
    }

    public static String createRandomWord(int len) {  // Convert to serial integer
        String name = "";
        for (int i = 0; i < len; i++) {
            int v = 1 + (int) (Math.random() * 26);
            char c = (char) (v + (i == 0 ? 'A' : 'a') - 1);
            name += c;
        }   return name;   
    }
    
    public static String createRandomIP(int length) {
        Random r = new Random();
        return r.nextInt(length) + "." + r.nextInt(length) + "." + r.nextInt(length) + "." + r.nextInt(length);
    }

    public static boolean validateIP(String inet4address) {
        InetAddressValidator validateMe = InetAddressValidator.getInstance();
        if (validateMe.isValidInet4Address(inet4address)) {
              // System.out.print("The IP address " + inet4address + " is valid\n");     
              return true;}
        else {System.out.print("The IP address " + inet4address + " is NOT valid\n"); 
        return false;}
    }

    static String seqString(int i) {
        return i < 0 ? "" : seqString((i / 26) - 1) + (char)(65 + i % 26);
    }

    public static void mapStrings() {
        for (int ii = 0; ii < App.amount; ii++) {
            App.hash_map.put(ii,seqString(ii)); 
            }
    }

    public static void mapIPs() {
        Integer counter = 0;
        String completeIP = "";
            parentloop:
                aloop: for (int a=1;a<254;a++) {if (counter == App.amount) {break parentloop;}
                        if (counter == App.amount) break aloop;
                    bloop: for (int b=0;b<254;b++) {if (counter == App.amount) break bloop;
                        cloop: for (int c=0;c<254;c++) {if (counter == App.amount) break cloop;
                            dloop: for (int d=1;d<254;d++) {if (counter >= App.amount) break dloop;
                                counter++;
                                completeIP = (a + "." + b + "." + c + "." + d);
                                // System.out.println(completeIP);
                                if (validateIP(completeIP)) {  //is VALID so carry on
                                    App.ip_map.put(createRandomWord(8), completeIP);}
                                else 
                                    {System.out.println("Invalid IP was created.  Please start over."); System.exit(0);}
                                if (counter >= App.amount) break;
                                }}}}
    }

    public static void mapIntegers () {
            for (int a=0;a<App.amount;a++) {
                App.integer_map.put(App.amount+a,a);
            }
    }
    
    public static void mapWords() {
        for (int w=0;w<(int)(App.amount/2);w++) {
            App.word1_map.put(w, "quick brown fox");
        }
        for (int w2=0;w2<(int)(App.amount/2);w2++) {
            App.word2_map.put(w2, "the lazy dog");
        }
    }

    public static void generateDistinctBINS() throws IOException {

        if (App.strings==true) {
            // CPC
            System.out.println("Generating Strings");
            CpcSketch sketch1out = new CpcSketch(App.ClgK);
            for (Map.Entry mapElement : App.hash_map.entrySet()) {
                String value = ((String)mapElement.getValue());  
                sketch1out.update(value);
                }
            FileOutputStream out1 = new FileOutputStream(App.cpcStrings);
            out1.write(sketch1out.toByteArray());
            out1.close();
            System.out.println("CPC Strings Created");


            HllSketch sketch4 = new HllSketch(App.HlgK);
            for (Map.Entry mapElementintegers : App.hash_map.entrySet()) {
                String value = (String)mapElementintegers.getValue();
                sketch4.update(value);  
                }
            FileOutputStream out4 = new FileOutputStream(App.hllStrings);
            out4.write(sketch4.toCompactByteArray());
            out4.close();

            
            UpdateSketch sketch7 = UpdateSketch.builder().build();
            for (Map.Entry mapElementintegers : App.hash_map.entrySet()) {
                String value = (String)mapElementintegers.getValue();
                sketch7.update(value);  
                }
            FileOutputStream out7 = new FileOutputStream(App.thetaStrings);
            out7.write(sketch7.compact().toByteArray());
            out7.close();
            }


        if (App.numbers==true) {

            System.out.println("Generating Integers");
            CpcSketch sketch2out = new CpcSketch(App.ClgK);
            for (Map.Entry mapElementintegers : App.integer_map.entrySet()) {
                Integer value = (Integer)mapElementintegers.getValue();
                sketch2out.update(value);  
                }
            FileOutputStream out1 = new FileOutputStream(App.cpcInts);
            out1.write(sketch2out.toByteArray());
            out1.close();
            System.out.println("CPC Ints Created");

            HllSketch sketch5 = new HllSketch(App.HlgK);
            for (Map.Entry mapElementintegers : App.integer_map.entrySet()) {
                Integer value = (Integer)mapElementintegers.getValue();
                sketch5.update(value);  
                }
            FileOutputStream out2 = new FileOutputStream(App.hllInts);
            out2.write(sketch5.toCompactByteArray());
            out2.close();

            UpdateSketch sketch8 = UpdateSketch.builder().build();
            for (Map.Entry mapElementintegers : App.integer_map.entrySet()) {
                Integer value = (Integer)mapElementintegers.getValue();
                sketch8.update(value);  
                }
            FileOutputStream out3 = new FileOutputStream(App.thetaInts);
            out3.write(sketch8.compact().toByteArray());
            out3.close();

            // Tuple is only integers 
            // Random rand = new Random();
            
            ArrayOfDoublesUpdatableSketch sketchInteger1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
            //for (int key = 0; key < App.amount; key++) sketchInteger1.update(key, new double[] {rand.nextGaussian()});
            for (Map.Entry mapElementintegers : App.integer_map.entrySet()) {
                Integer value = (Integer)mapElementintegers.getValue();
                Integer key =   (Integer)mapElementintegers.getKey();
                sketchInteger1.update(key, new double[] {value});  
                }
            System.out.println("Writing Tuple File");
            FileOutputStream outtuple = new FileOutputStream(App.tupleint1);
            outtuple.write(sketchInteger1.compact().toByteArray());
            outtuple.close();

            /* ArrayOfDoublesUpdatableSketch sketchInteger2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
            // for (int key = 0; key < App.amount; key++) sketchInteger2.update(key, new double[] {rand.nextGaussian()});
            for (Map.Entry mapElementintegers : App.integer_map.entrySet()) {
                Integer value = (Integer)mapElementintegers.getValue();
                Integer key = (Integer)mapElementintegers.getKey();
                sketchInteger2.update(key, new double[] {value}); // sketch8.update(value);  
                }
            FileOutputStream outtuple2 = new FileOutputStream(App.tupleint2);
            outtuple2.write(sketchInteger2.compact().toByteArray());
            outtuple2.close();

            ArrayOfDoublesUpdatableSketch sketchInteger3 = new ArrayOfDoublesUpdatableSketchBuilder().build();
            // for (int key = 0; key < App.amount; key++) sketchInteger3.update(key, new double[] {rand.nextGaussian()});
            for (Map.Entry mapElementintegers : App.integer_map.entrySet()) {
                Integer value = (Integer)mapElementintegers.getValue();
                Integer key = (Integer)mapElementintegers.getKey();
                sketchInteger3.update(key, new double[] {value}); // sketch8.update(value);  
                }
            FileOutputStream out3 = new FileOutputStream(App.tupleint3);
            out3.write(sketchInteger3.compact().toByteArray());
            out3.close(); */

            }
            
        if (App.ips==true) {
            System.out.println("Generating IPs");
            CpcSketch sketch3out = new CpcSketch(App.ClgK);
            for (Map.Entry mapElementIP : App.ip_map.entrySet()) {
                String value = (String)mapElementIP.getValue();
                sketch3out.update(value);  
                }
            FileOutputStream out3 = new FileOutputStream(App.cpcIPs);
            out3.write(sketch3out.toByteArray());
            out3.close();
            System.out.println("CPC IP Created");

            HllSketch sketch6 = new HllSketch(App.HlgK);
            for (Map.Entry mapElementintegers : App.ip_map.entrySet()) {
                String value = (String)mapElementintegers.getValue();
                sketch6.update(value);  
                }
            FileOutputStream out6 = new FileOutputStream(App.hllIPs);
            out6.write(sketch6.toCompactByteArray());
            out6.close();

            UpdateSketch sketch9 = UpdateSketch.builder().build();
            for (Map.Entry mapElementintegers : App.ip_map.entrySet()) {
                String value = (String)mapElementintegers.getValue();
                sketch9.update(value);  
                }
            FileOutputStream out9 = new FileOutputStream(App.thetaIPs);
            out9.write(sketch9.compact().toByteArray());
            out9.close();
        }
    }
}