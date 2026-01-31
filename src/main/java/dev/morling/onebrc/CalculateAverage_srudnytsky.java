package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_srudnytsky {
    private static final String FILE = "measurements.txt";

    static class Stats {
        double min = 100.0, max = -100.0, sum = 0.0;
        long count = 0;

        void update(double val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            sum += val;
            count++;
        }

        void merge(Stats other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        public String toString() {
            return String.format(Locale.US, "%.1f/%.1f/%.1f", min, round(sum, count), max);
        }
    }

    private static double round(double sum, double count) {
        double value = (Math.round(sum * 10.0) / 10.0) / count;
        return Math.round(value * 10.0) / 10.0;
    }

    public static void main(String[] args) throws Exception {
        File file = new File(FILE);
        long length = file.length();
        int cores = Runtime.getRuntime().availableProcessors();
        ExecutorService service = Executors.newFixedThreadPool(cores);

        List<Future<Map<String, Stats>>> futures = new ArrayList<>();
        long chunkSize = length / cores;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long start = 0;
            for (int i = 0; i < cores; i++) {
                long end = (i == cores - 1) ? length : start + chunkSize;
                if (end < length) {
                    raf.seek(end);
                    while (end < length && raf.read() != '\n')
                        end++;
                }
                long finalStart = start;
                long finalEnd = end;
                futures.add(service.submit(() -> process(finalStart, finalEnd)));
                start = end;
            }

            TreeMap<String, Stats> finalMap = new TreeMap<>();
            for (Future<Map<String, Stats>> f : futures) {
                f.get().forEach((k, v) -> finalMap.computeIfAbsent(k, s -> new Stats()).merge(v));
            }
            System.out.println(finalMap);
        } finally {
            service.shutdown();
        }
    }

    private static Map<String, Stats> process(long start, long end) throws IOException {
        Map<String, Stats> map = new HashMap<>(1000);
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            MappedByteBuffer bb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, end - start);
            byte[] nameBuf = new byte[100];

            while (bb.hasRemaining()) {
                int p = 0;
                byte b;
                while ((b = bb.get()) != ';')
                    nameBuf[p++] = b;

                String name = new String(nameBuf, 0, p, StandardCharsets.UTF_8);

                double temp = parseFast(bb);

                map.computeIfAbsent(name, k -> new Stats()).update(temp);
            }
        }
        return map;
    }

    private static double parseFast(MappedByteBuffer bb) {
        int res = 0;
        boolean neg = false;
        byte b = bb.get();
        if (b == '-') {
            neg = true;
            b = bb.get();
        }
        while (b != '\n') {
            if (b >= '0' && b <= '9')
                res = res * 10 + (b - '0');
            if (!bb.hasRemaining())
                break;
            b = bb.get();
        }
        return (neg ? -res : res) / 10.0;
    }
}
