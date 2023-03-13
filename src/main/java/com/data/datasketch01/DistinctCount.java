package com.data.datasketch01;

// Common
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Locale;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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


public class DistinctCount {

    public static void cpcSketchCreate()  throws FileNotFoundException, IOException  {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // System.out.println(App.strings+ " " + App.numbers + " " + App.ips);
        CpcUnion union = new CpcUnion(App.ClgK);
        CpcUnion test_union = new CpcUnion(11);
        Double tally = 0.0;

        if (App.strings==true) {
            System.out.println("Loading CPC Strings");
            FileInputStream in1 = new FileInputStream(App.cpcStrings);
            byte[] bytes1 = new byte[in1.available()];
            in1.read(bytes1);
            in1.close();
            CpcSketch sketch1 = CpcSketch.heapify(Memory.wrap(bytes1));
            union.update(sketch1);
            System.out.println("Strings  :" + sketch1.getEstimate());
            test_union.update(sketch1);
            tally = tally + (sketch1.getEstimate());
        }

        if (App.numbers==true) {
            System.out.println("Loading CPC Integers");
            FileInputStream in2 = new FileInputStream(App.cpcInts);
            byte[] bytes2 = new byte[in2.available()];
            in2.read(bytes2);
            in2.close();
            CpcSketch sketch2 = CpcSketch.heapify(Memory.wrap(bytes2));
            union.update(sketch2);
            System.out.println("Integers :" + sketch2.getEstimate());
            test_union.update(sketch2);
            tally = tally + (sketch2.getEstimate());
        }

        if (App.ips==true) {
            System.out.println("Loading CPC IPs");
            FileInputStream in3 = new FileInputStream(App.cpcIPs);
            byte[] bytes3 = new byte[in3.available()];
            in3.read(bytes3);
            in3.close();
            CpcSketch sketch3 = CpcSketch.heapify(Memory.wrap(bytes3));
            union.update(sketch3);
            System.out.println("IPs      :" + sketch3.getEstimate());
            test_union.update(sketch3);
            tally = tally + (sketch3.getEstimate());
        }
        
        CpcSketch test_result = test_union.getResult();

        CpcSketch cpc_result = union.getResult();
        // System.out.println("Total : "+ cpc_result.getEstimate());
        String cpc_result_string = String.valueOf((cpc_result.getEstimate()));
        Double cpc_result_final = Double.parseDouble(cpc_result_string);
        System.out.println("TEST 1:  " + test_result.getEstimate());
        System.out.println("TEST 2:  " + cpc_result_final);
        System.out.println("Tally : " + tally);
        App.sketchTable.put("Result:", "CPC", NumberFormat.getNumberInstance(Locale.US).format(cpc_result_final));
        App.timerTable.put("Time", "CPC", stopwatch.toString());
        stopwatch.reset();
        // return (NumberFormat.getNumberInstance(Locale.US).format(cpc_result.getEstimate()).toString());
    }

    public static void hllSketchCreate() throws FileNotFoundException, IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        org.apache.datasketches.hll.Union hll_union = new org.apache.datasketches.hll.Union(App.HlgK);
        
        if (App.strings==true) {
            System.out.println("Loading HLL Strings");
            FileInputStream in1 = new FileInputStream(App.hllStrings);
            byte[] bytes1 = new byte[in1.available()];
            in1.read(bytes1);
            in1.close();
            HllSketch sketch1 = HllSketch.heapify(Memory.wrap(bytes1));
            hll_union.update(sketch1);
            }

        if (App.numbers==true) {
            System.out.println("Loading HLL Integers");
            FileInputStream in2 = new FileInputStream(App.hllInts);
            byte[] bytes2 = new byte[in2.available()];
            in2.read(bytes2);
            in2.close();
            HllSketch sketch2 = HllSketch.heapify(Memory.wrap(bytes2));
            hll_union.update(sketch2);
            }

        if (App.ips==true) {
            System.out.println("Loading HLL IPs");
            FileInputStream in3 = new FileInputStream(App.hllIPs);
            byte[] bytes3 = new byte[in3.available()];
            in3.read(bytes3);
            in3.close();
            HllSketch sketch3 = HllSketch.heapify(Memory.wrap(bytes3));
            hll_union.update(sketch3);
        }

        HllSketch unionResult = hll_union.getResult(TgtHllType.HLL_4);
        App.sketchTable.put("Result:", "HLL", NumberFormat.getNumberInstance(Locale.US).format(unionResult.getEstimate()).toString());
        App.timerTable.put("Time", "HLL", stopwatch.toString());
        stopwatch.reset();
    }

    public static void thetaSketchCreate() throws FileNotFoundException, IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Intersection intersection = SetOperation.builder().buildIntersection();
        org.apache.datasketches.theta.Union theta_union = SetOperation.builder().buildUnion();
        Double sketch1double = 0.0;
        Double sketch2double = 0.0;
        Double sketch3double = 0.0;

        if (App.strings==true) {
            System.out.println("Loading Theta Strings");
            FileInputStream in1 = new FileInputStream(App.thetaStrings);
            byte[] bytes1 = new byte[in1.available()];
            in1.read(bytes1);
            in1.close(); 
            Sketch sketch1 = Sketches.wrapSketch(Memory.wrap(bytes1));
            theta_union.update(sketch1);
            intersection.update(sketch1);
            sketch1double = sketch1.getEstimate();
        }

        if (App.numbers==true) {
            System.out.println("Loading Theta Integers");
            FileInputStream in2 = new FileInputStream(App.thetaInts);
            byte[] bytes2 = new byte[in2.available()];
            in2.read(bytes2);
            in2.close();
            Sketch sketch2 = Sketches.wrapSketch(Memory.wrap(bytes2));
            theta_union.update(sketch2);
            intersection.update(sketch2);
            sketch2double = sketch2.getEstimate();
        }

        if (App.ips==true) {
            System.out.println("Loading Theta IPs");
            FileInputStream in3 = new FileInputStream(App.thetaIPs);
            byte[] bytes3 = new byte[in3.available()];
            in3.read(bytes3);
            in3.close();
            Sketch sketch3 = Sketches.wrapSketch(Memory.wrap(bytes3));
            theta_union.update(sketch3);
            intersection.update(sketch3); 
            sketch3double = sketch3.getEstimate();
        }    

        Sketch unionResult = theta_union.getResult();
        App.sketchTable.put("Result:", "Theta 1", NumberFormat.getNumberInstance(Locale.US).format(unionResult.getEstimate()).toString());
        double thetaTotal = sketch1double+sketch2double+sketch3double; 
        App.sketchTable.put("Result:", "Theta 2", NumberFormat.getNumberInstance(Locale.US).format(thetaTotal).toString());
        App.timerTable.put("Time", "Theta", stopwatch.toString());
        stopwatch.reset();
    }

    public static void tupleSketchCreate() throws FileNotFoundException, IOException {
        System.out.println("Loading Tuple Integers");
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        org.apache.datasketches.tuple.ArrayOfDoublesUnion tupleUnion = new ArrayOfDoublesSetOperationBuilder().buildUnion();

        FileInputStream in1 = new FileInputStream(App.tupleint1);
        byte[] bytes1 = new byte[in1.available()];
        in1.read(bytes1);
        in1.close();
        ArrayOfDoublesSketch sketch1 = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(bytes1));
        tupleUnion.update(sketch1);

        /* System.out.println("Loading Tuple Integers 2");
        FileInputStream in2 = new FileInputStream(App.tupleint2);
        byte[] bytes2 = new byte[in2.available()];
        in2.read(bytes2);
        in2.close();
        ArrayOfDoublesSketch sketch2 = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(bytes2));
        tupleUnion.update(sketch2);

        
        System.out.println("Loading Tuple Integers 3");
        FileInputStream in3 = new FileInputStream(App.tupleint3);
        byte[] bytes3 = new byte[in3.available()];
        in3.read(bytes3);
        in3.close();
        ArrayOfDoublesSketch sketch3 = ArrayOfDoublesSketches.wrapSketch(Memory.wrap(bytes3));
        tupleUnion.update(sketch3); */


        ArrayOfDoublesSketch unionResult = tupleUnion.getResult();
        App.sketchTable.put("Result:", "Tuple", NumberFormat.getNumberInstance(Locale.US).format(unionResult.getEstimate()).toString());
        UpdateDoublesSketch quantilesSketch = DoublesSketch.builder().build();
        ArrayOfDoublesSketchIterator it = unionResult.iterator();
        while (it.next()) {
            quantilesSketch.update(it.getValues()[0]);
        }
        App.timerTable.put("Time", "Tuple", stopwatch.toString());
        stopwatch.reset();
    }
}
