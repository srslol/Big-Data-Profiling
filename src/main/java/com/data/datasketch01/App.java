package com.data.datasketch01;

// Version 0.3.2
// Scott Myers
// Wallaroo.ai 
// Using DataSketches to get distinct counts of specific datatypes 
// Quarter 4, 2021
// Updated Nov 30:  Changed IP range from random to serial.  Created third sketch for three hashmaps.
// Updated Dec 4:   Added HyperLogLog analysis using same data for direct comparison 
// Updated Dec 5:   Added Theta
// Updated Dec 6:   Attempting Tuple Sketch - Running into dependency problems 
// Updated Dec 7:   Added filesize / Switched to Linux
// Updated Dec 8:   Added to GitLab
// Updated Dec 9:   Added Frequent Values
// Updated Dec 10:  Add sentences/larger value strings in Most Frequent 
// Updated Dec 12:  Filenames sorted, table results
// Updated Dec 13:  Adding Reservoir Sampling 
// Updated Dec 15:  Modularized in functions for easier use in other programs.  
// Updated Dec 16:  Cleaned up, ready for review.
// Updated Dec 17:  Adding Guava Stopwatch
// Updated Dec 21:  Decoupling into modules to make easier to move to prod
// Updated Jan 03:  Added CSV module

// Common Imports

import java.util.concurrent.TimeUnit;
import java.util.Scanner;
import java.util.HashMap;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Table;
import com.google.common.collect.HashBasedTable;

public class App {
    
    public static boolean numbers ;
    public static boolean strings ;
    public static boolean ips ; 
    public static int amount = 1_000_000;
    public static int ClgK = 11;          //   <----   11 Default 
    public static int HlgK = 11;
    public static HashMap<String, String> ip_map = new HashMap<String, String>();
    public static HashMap<Integer, String> hash_map = new HashMap<Integer, String>();
    public static HashMap<Integer, Integer> integer_map = new HashMap<Integer, Integer>();
    public static HashMap<Integer, String> word1_map = new HashMap<Integer, String>();
    public static HashMap<Integer, String> word2_map = new HashMap<Integer, String>();
    public static Table<String, String, String> dataTable = HashBasedTable.create();
    public static Table<String, String, String> sketchTable = HashBasedTable.create();
    public static Table<String, String, String> timerTable = HashBasedTable.create();
    public static Table<String, String, String> MostFreqTable = HashBasedTable.create();

    public static String manyLines = "------------------------------------------------------------------------------";
    public static String workpath =                 "bins/";
    public static String reportpath =               "report/";
    public static String reportfile =               "report/report.txt";
    public static String cpcIPs =workpath+          "DC - CPC-Integers.bin";
    public static String cpcInts=workpath+          "DC - CPC-Integers.bin";
    public static String cpcStrings=workpath+       "DC - CPC-Strings.bin";
    public static String freqstring1=workpath+      "Frequency-Strings1.bin";
    public static String freqstring2=workpath+      "Frequency-Strings2.bin";
    public static String freqstring3=workpath+      "Frequency-Strings3.bin";
    public static String hllIPs=workpath+           "DC - HLL-IPs.bin";
    public static String hllStrings=workpath+       "DC - HLL-Strings.bin";
    public static String hllInts =workpath+         "DC - HLL-Integers.bin";
    public static String quant1=workpath+           "Quantiles1.bin";
    public static String quant2 =workpath+          "Quantiles2.bin";
    public static String thetaIPs =workpath+        "DC - Theta-IPs.bin";
    public static String thetaInts =workpath+       "DC - Theta-Integers.bin";
    public static String thetaStrings =workpath+    "DC - Theta-Strings.bin";
    public static String tupleint1=workpath+        "DC - Tuple-Integers1.bin";
    public static String tupleint2=workpath+        "DC - Tuple-Integers2.bin";
    public static String tupleint3=workpath+        "DC - Tuple-Integers3.bin";
    public static String reserv1=workpath+          "Reservoir1.bin";
    public static String reserv2=workpath+          "Reservoir2.bin";
    public static String csv_path=                  "csv/";
    public static String csv_cpc=csv_path+   "csv_cpc.bin";
    public static String csv_hll=csv_path+   "csv_hll.bin";
    public static String csv_theta=csv_path+   "csv_theta.bin";
    public static String csv_file=csv_path+     "test.csv";
    
    public static void main(String[] args) throws Exception {
        // Clear out Report Folder        
        Dashboard.clearReportFolder();
        System.out.println("Report Folder Cleared");
        // TimeUnit.SECONDS.sleep(1); // let the file delete first
        System.out.println("--------------------------------------");
        System.out.println("Please select a variable type to test:");
        System.out.println("1.  Integers <Tuples Integers Only>");
        System.out.println("2.  Strings  ");
        System.out.println("3.  IP Addresses ");
        System.out.println("4.  All 3 Types (Triples Values)");
        System.out.println("5.  Run All Tests");
        System.out.println("6.  Load CSV file");
        System.out.println("--------------------------------------");
        Scanner console = new Scanner(System.in);
        // System.out.println(">");
        String selection = console.nextLine();
/*
        numbers = false;
        ips = false;
        strings = false;
*/
        switch (selection) {
            case "1":
                numbers = true;
                strings = false;
                ips = false;
                Generate.mapIntegers();
                break;

            case "2":
                strings = true;
                numbers = false;
                ips = false;
                Generate.mapStrings();
                break;

            case "3":
                ips = true;
                strings = false;
                numbers = false;
                Generate.mapIPs();
                break;

            case "4":
                numbers = true;
                strings = true;
                ips = true;
                Generate.mapStrings();
                Generate.mapIntegers();
                Generate.mapIPs();
                break;

            case "5":
                numbers = true;
                strings = true;
                ips = true;
                Generate.mapStrings();
                Generate.mapIntegers();
                Generate.mapIPs();
                console.close();
                Generate.generateDistinctBINS();
                Stopwatch mainTimer = Stopwatch.createStarted();
                DistinctCount.cpcSketchCreate();
                DistinctCount.hllSketchCreate();
                DistinctCount.thetaSketchCreate();
                if (numbers==true) {
                    DistinctCount.tupleSketchCreate();
                }
                
                Quantiles.quantileCreateBINS();
                Quantiles.quantileSketchCreate();

                MostFrequent.frequencyCreateBINS();
                MostFrequent.frequencySketchCreate();

                Sampling.sampleCreateBINS();
                Sampling.samplingSketchCreate();
                Dashboard.dashboard(); 
                System.out.println(manyLines);
                System.out.println("Total Time: " + mainTimer.toString());
                System.out.println(manyLines);
                System.exit(0);    
            
            case "6":
                CSVdata.readCSVbyLine(csv_file);
                System.exit(0);

            default:
                System.out.println("Invalid");
                System.exit(0);
        }

        System.out.println("----------------------------------------");
        System.out.println("Please select a sketch type to test:  ");
        System.out.println("1.  Distinct  Counting ");
        System.out.println("2.  Most Frequent  ");
        System.out.println("3.  Quantiles and Histograms ");
        System.out.println("4.  Sampling ");
        System.out.println("5.  All Tests ");
        System.out.println("--------------------------------------");
        
        String selectionb = console.next();
        // System.out.println(">");
        System.out.println("Please see /report/report.txt");
        Stopwatch mainTimer = Stopwatch.createStarted();
        switch (selectionb) {
            case "1":
                Generate.generateDistinctBINS();
                DistinctCount.cpcSketchCreate();
                DistinctCount.hllSketchCreate();
                DistinctCount.thetaSketchCreate();
                if (numbers==true ) {
                    DistinctCount.tupleSketchCreate();
                }
                break;

            case "2":
                MostFrequent.frequencyCreateBINS();
                MostFrequent.frequencySketchCreate();
                break;

            case "3":
                Quantiles.quantileCreateBINS();
                Quantiles.quantileSketchCreate();
                break;

            case "4":
                Sampling.sampleCreateBINS();
                Sampling.samplingSketchCreate();
                break;

            case "5":
                Generate.generateDistinctBINS();

                DistinctCount.cpcSketchCreate();
                DistinctCount.hllSketchCreate();
                DistinctCount.thetaSketchCreate();
                if (numbers==true) {
                    DistinctCount.tupleSketchCreate();
                }
                
                Quantiles.quantileCreateBINS();
                Quantiles.quantileSketchCreate();

                MostFrequent.frequencyCreateBINS();
                MostFrequent.frequencySketchCreate();

                Sampling.sampleCreateBINS();
                Sampling.samplingSketchCreate();
                break;

            default:
                System.out.println("Invalid");
                System.exit(0);
        }
        
        console.close();
        Dashboard.dashboard(); 
        System.out.println(manyLines);
        System.out.println("Total Time: " + mainTimer.toString());
        System.out.println(manyLines);
        }
    }