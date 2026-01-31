/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_srudnytsky {
    private static final String FILE = "measurements.txt";

    static class Stats {
        int min = 1000, max = -1000;
        long sum = 0, count = 0;

        void update(int val) {
            if (val < min)
                min = val;
            if (val > max)
                max = val;
            sum += val;
            count++;
        }

        void merge(Stats other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        @Override
        public String toString() {
            return String.format(Locale.US, "%.1f/%.1f/%.1f",
                    min / 10.0, (Math.round((double) sum / count) / 10.0), max / 10.0);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        File file = new File(FILE);
        long length = file.length();
        int cores = Runtime.getRuntime().availableProcessors();

        if (length < 1000000)
            cores = 1;

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
                    end = raf.getFilePointer();
                }
                long finalStart = start;
                long finalEnd = end;
                if (finalEnd > finalStart) {
                    futures.add(service.submit(() -> processChunk(finalStart, finalEnd)));
                }
                start = end;
            }

            TreeMap<String, Stats> finalMap = new TreeMap<>();
            for (Future<Map<String, Stats>> f : futures) {
                f.get().forEach((k, v) -> finalMap.computeIfAbsent(k, s -> new Stats()).merge(v));
            }

            System.out.println(finalMap.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(", ", "{", "}")));
        }
        finally {
            service.shutdown();
        }
    }

    private static Map<String, Stats> processChunk(long start, long end) throws IOException {
        Map<String, Stats> map = new HashMap<>(1024);
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            MappedByteBuffer bb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, end - start);
            byte[] nameBuf = new byte[128];

            while (bb.hasRemaining()) {
                int p = 0;
                byte b;
                while (bb.hasRemaining() && (b = bb.get()) != ';') {
                    nameBuf[p++] = b;
                }
                String name = new String(nameBuf, 0, p, StandardCharsets.UTF_8);

                int val = 0;
                boolean neg = false;
                while (bb.hasRemaining()) {
                    b = bb.get();
                    if (b == '-')
                        neg = true;
                    else if (b >= '0' && b <= '9')
                        val = val * 10 + (b - '0');
                    else if (b == '\n' || b == '\r') {
                        if (b == '\r' && bb.hasRemaining() && bb.get(bb.position()) == '\n')
                            bb.get();
                        break;
                    }
                }
                map.computeIfAbsent(name, k -> new Stats()).update(neg ? -val : val);
            }
        }
        return map;
    }
}