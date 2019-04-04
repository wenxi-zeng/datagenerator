package edu.utdallas.iot;

import com.jmatio.io.MatFileReader;
import com.jmatio.types.MLArray;
import com.jmatio.types.MLChar;
import com.jmatio.types.MLDouble;
import com.jmatio.types.MLStructure;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Generator {

    private String host = "http://10.176.128.241:8086";
    private String username = "alien";
    private String password = "alien1";
    private String db = "bearing";
    private InfluxDB influxDB;
    private static int BW = 256; // buffer window
    private static int MW = BW * 4; // merge window
    private static int FILE_SIZE = 20;
    private static int INTERVAL = 1;

    private static int LABEL_BASELINE = 0;
    private static int LABEL_OUTER_RACE_FAULT = 1;
    private static int LABEL_INNER_RACE_FAULT = 2;

    private static String ATTR_SR = "sr";
    private static String ATTR_GS = "gs";
    private static String ATTR_LOAD = "load";
    private static String ATTR_RATE = "rate";

    private String[] filenames = new String[] {
        "mats" + File.separator + "baseline_1.mat",
        "mats" + File.separator + "baseline_2.mat",
        "mats" + File.separator + "baseline_3.mat",

        "mats" + File.separator + "OuterRaceFault_1.mat",
        "mats" + File.separator + "OuterRaceFault_2.mat",
        "mats" + File.separator + "OuterRaceFault_3.mat",

        "mats" + File.separator + "InnerRaceFault_vload_1.mat",
        "mats" + File.separator + "InnerRaceFault_vload_2.mat",
        "mats" + File.separator + "InnerRaceFault_vload_3.mat",
        "mats" + File.separator + "InnerRaceFault_vload_4.mat",
        "mats" + File.separator + "InnerRaceFault_vload_5.mat",
        "mats" + File.separator + "InnerRaceFault_vload_6.mat",
        "mats" + File.separator + "InnerRaceFault_vload_7.mat",

        "mats" + File.separator + "OuterRaceFault_vload_1.mat",
        "mats" + File.separator + "OuterRaceFault_vload_2.mat",
        "mats" + File.separator + "OuterRaceFault_vload_3.mat",
        "mats" + File.separator + "OuterRaceFault_vload_4.mat",
        "mats" + File.separator + "OuterRaceFault_vload_5.mat",
        "mats" + File.separator + "OuterRaceFault_vload_6.mat",
        "mats" + File.separator + "OuterRaceFault_vload_7.mat"
    };

    private MLStructure[] readers = new MLStructure[FILE_SIZE];
    private int[] lengths = new int[FILE_SIZE];
    private double[] probs = new double[FILE_SIZE];
    private int totalLength;
    private Random random = new Random();
    private int[] cursors = new int[FILE_SIZE];
    private long timestamp;

    private void connect() {
        influxDB = InfluxDBFactory.connect(host, username, password);
        influxDB.setDatabase(db);
        influxDB.enableBatch(BatchOptions.DEFAULTS.actions(BW).exceptionHandler(
                (failedPoints, throwable) -> throwable.printStackTrace())
        );
    }

    private void loadMatFiles() throws IOException {
        totalLength = 0;
        for (int i = 0; i < filenames.length; i++) {
            MatFileReader reader = new MatFileReader(filenames[i]);
            readers[i] = (MLStructure) reader.getMLArray(db);
            int len = readers[i].getField(ATTR_GS).getSize();
            lengths[i] = len - len % MW;
            totalLength += lengths[i];
            cursors[i] = 0;
        }

        probs[0] = lengths[0] * .1 / totalLength;
        for (int i = 1; i < probs.length; i++) {
            probs[i] = probs[i - 1] + lengths[i] * .1 / totalLength;
        }
    }

    private int selectFile() {
        double prob = random.nextDouble();
        int min, max;

        if (prob < 0.7) {
            min = 0;
            max = 2;
        }
        else {
            min = 3;
            max = 19;
        }

        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    private int generatepPoints(int index, String metric) {
        int len = readers[index].getField(ATTR_GS).getSize();
        int start = cursors[index];
        int end = cursors[index] + MW;
        if (MW > len - cursors[index]) {
            cursors[index] = 0;
        }

        BatchPoints batchPoints = BatchPoints
                .database(db)
                .build();
        for (int i = start; i < end; i++ ) {
            Point.Builder builder = Point.measurement(metric)
                    .time(timestamp++, TimeUnit.MILLISECONDS)
                    .addField("label", getLabel(index))
                    .addField(ATTR_SR, ((MLDouble)readers[index].getField(ATTR_SR)).getArray()[0][0])
                    .addField(ATTR_RATE, ((MLDouble)readers[index].getField(ATTR_RATE)).getArray()[0][0])
                    .addField(ATTR_GS, ((MLDouble)readers[index].getField(ATTR_GS)).getArray()[i][0]);

            if (readers[index].getField(ATTR_LOAD) instanceof MLChar) {
                builder.addField(ATTR_LOAD, ((MLChar) readers[index].getField(ATTR_LOAD)).getString(0));
            }
            else {
                builder.addField(ATTR_LOAD, String.valueOf(((MLDouble) readers[index].getField(ATTR_LOAD)).getArray()[0][0]));
            }

            batchPoints.point(builder.build());
        }
        influxDB.write(batchPoints);
        System.out.println(batchPoints.getPoints().size() + " points were written to influx");
        return MW;
    }

    private int getLabel(int index) {
        if (index < 3)
            return LABEL_BASELINE;
        else if (index < 6)
            return LABEL_OUTER_RACE_FAULT;
        else if (index < 13)
            return LABEL_INNER_RACE_FAULT;
        else
            return LABEL_OUTER_RACE_FAULT;
    }

    public static void main(String[] args) {
        Generator generator = new Generator();
        generator.connect();

        try {
            generator.loadMatFiles();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        int offlineCount = 0;
        int offlineTotal = generator.totalLength / 2;
        generator.timestamp = 0;
        while (offlineCount < offlineTotal) {
            int index = generator.selectFile();
            offlineCount += generator.generatepPoints(index, "offline");
        }

        generator.timestamp = 0;
        while (true) {
            int index = generator.selectFile();
            generator.generatepPoints(index, "online");
        }
    }
}
