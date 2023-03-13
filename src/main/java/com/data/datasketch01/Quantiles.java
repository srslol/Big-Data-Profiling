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
import java.util.Random;
import java.util.Arrays;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.datasketches.memory.Memory;

// Quantiles
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

public class Quantiles {

    public static void quantileCreateBINS () throws FileNotFoundException, IOException {
        Random rand = new Random();
        
        UpdateDoublesSketch sketch1 = DoublesSketch.builder().build(); // default k=128
        for (int i = 0; i < App.amount; i++) {
            sketch1.update(rand.nextGaussian()); // mean=0, stddev=1
        }
        FileOutputStream qOut1 = new FileOutputStream(App.quant1);
        qOut1.write(sketch1.toByteArray());
        qOut1.close();

        UpdateDoublesSketch sketch2 = DoublesSketch.builder().build(); // default k=128
        for (int i = 0; i < App.amount; i++) {
            sketch2.update(rand.nextGaussian() + 1); // shift the mean for the second sketch
        }
        FileOutputStream qOut2 = new FileOutputStream(App.quant2);
        qOut2.write(sketch2.toByteArray());
        qOut2.close();

    }


    public static void quantileSketchCreate() throws FileNotFoundException, IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        DoublesUnion union = DoublesUnion.builder().build(); // default k=128    
                
        FileInputStream qin1 = new FileInputStream(App.quant1);
        byte[] bytes1 = new byte[qin1.available()];
        qin1.read(bytes1);
        qin1.close();
        DoublesSketch qsketch1 = DoublesSketch.wrap(Memory.wrap(bytes1));
        union.update(qsketch1);
    
        FileInputStream qin2 = new FileInputStream(App.quant2);
        byte[] bytes2 = new byte[qin2.available()];
        qin2.read(bytes2);
        qin2.close();
        DoublesSketch qsketch2 = DoublesSketch.wrap(Memory.wrap(bytes2));
        union.update(qsketch2);

        DoublesSketch result = union.getResult();
        String x = Arrays.toString(result.getQuantiles(new double[] {0, 0.5, 1}));
        x = x.replace("]", "");
        x = x.replace("[", "");
        x = x.replace(" ", "");
        String[] qSplit = x.split(",",3);
        Integer aa =0;
        for (aa=0;aa<qSplit.length;aa++) {
            qSplit[aa] = String.format("%.4g", Double.parseDouble(qSplit[aa]));
        }
        App.dataTable.put("Result:", "Quan Min", qSplit[0]);
        App.dataTable.put("Result:", "Quan Med", qSplit[1]);
        App. dataTable.put("Result:", "Quan Max", qSplit[2]);
        
        String x2 = (Arrays.toString(result.getPMF(new double[] {-2, 0, 2})));
        x2 = x2.replace("]", "");
        x2 = x2.replace("[", "");
        x2 = x2.replace(" ", "");
        String[] qSplit2 = x2.split(",");
        Integer aa2=0;
        for (aa2=0;aa2<qSplit2.length;aa2++) {
            qSplit2[aa2] = String.format("%.4g", Double.parseDouble(qSplit2[aa2]));
            String probItem = "Prob " + aa2.toString();
            App.dataTable.put("Result:", probItem, qSplit2[aa2]);
        }

        double[] histogram = result.getPMF(new double[] {-2, 0, 2});
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] *= result.getN(); // scale the fractions by the total count of values 
        }

        String x3 = Arrays.toString(histogram); 
        //System.out.println(x3);
        x3 = x3.replace("]", "");
        x3 = x3.replace("[", "");
        x3 = x3.replace(" ", "");
        String[] qSplit3 = x3.split(",");
        Integer aa3=0;
        for (aa3=0;aa3<qSplit3.length;aa3++) {
            qSplit3[aa3] = String.format("%.5g", Double.parseDouble(qSplit3[aa3]));
            String probItem = "Freq " + aa3.toString();
            App.dataTable.put("Result:", probItem, qSplit3[aa3]);
        }
        App.timerTable.put("Time", "Quantile", stopwatch.toString());
        stopwatch.reset();
    }
}
