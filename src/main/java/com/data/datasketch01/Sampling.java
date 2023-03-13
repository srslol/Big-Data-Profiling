package com.data.datasketch01;

// Common
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.File;
import com.google.common.base.Stopwatch;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Random;
import org.apache.datasketches.memory.Memory;

// Reservoir Sampling
import org.apache.datasketches.ArrayOfNumbersSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;
import org.apache.datasketches.ArrayOfLongsSerDe;


public class Sampling {

    public static void sampleCreateBINS() throws FileNotFoundException, IOException {
        int k = 8192;

        ReservoirItemsSketch<Long> sketch1o = ReservoirItemsSketch.newInstance(k);
        for (long key = 0; key < App.amount; key++) { sketch1o.update(key); }
        FileOutputStream out1 = new FileOutputStream(new File(App.reserv1));
        out1.write(sketch1o.toByteArray(new ArrayOfLongsSerDe()));
        out1.close();

        ReservoirItemsSketch<Long> sketch2o = ReservoirItemsSketch.newInstance(k);
        for (long key = 0; key < App.amount; key++) { sketch2o.update(key); }
        FileOutputStream out2 = new FileOutputStream(new File(App.reserv2));
        out2.write(sketch2o.toByteArray(new ArrayOfLongsSerDe()));
        out2.close();
    }

    public static void samplingSketchCreate() throws FileNotFoundException, IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();

        FileInputStream in1 = new FileInputStream(new File(App.reserv1));
        byte[] bytes1 = new byte[in1.available()];
        in1.read(bytes1);
        in1.close();
        ReservoirItemsSketch<Long> sketch1 = ReservoirItemsSketch.heapify(Memory.wrap(bytes1), 
                                                                            new ArrayOfLongsSerDe());

        FileInputStream in2 = new FileInputStream(new File(App.reserv2));
        byte[] bytes2 = new byte[in2.available()];
        in2.read(bytes2);
        in2.close();
        ReservoirItemsSketch<Long> sketch2 = ReservoirItemsSketch.heapify(Memory.wrap(bytes2),
                                                                            new ArrayOfLongsSerDe());

        int k2 = sketch1.getK();
        ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(k2);
        union.update(sketch1);
        union.update(sketch2);
        ReservoirItemsSketch<Long> unionResult = union.getResult();

        Long[] samples = unionResult.getSamples();
        String loop, results;
        for (int i = 0; i < 10; i++) {
            results = "Sample-" + NumberFormat.getNumberInstance(Locale.US).format(i).toString();
            loop = NumberFormat.getNumberInstance(Locale.US).format(samples[i]).toString();
            App.dataTable.put("Result:", results, loop);
        }
        App.timerTable.put("Time", "Sampling", stopwatch.toString());
        
        Random rand = new Random();
        int sample_random;
        int sketch_length = samples.length;
        for (int i = 0; i < 10; i++) {
            sample_random = rand.nextInt(sketch_length);
            results = "RandSample-" + NumberFormat.getNumberInstance(Locale.US).format(i).toString();
            loop = NumberFormat.getNumberInstance(Locale.US).format(samples[sample_random]).toString();
            App.dataTable.put("Result:", results, loop);
        }
        stopwatch.reset();
    }
}
