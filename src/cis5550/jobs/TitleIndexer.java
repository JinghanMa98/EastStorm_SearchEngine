// Vivian Final Column-Based TitleIndexer (NO POSITION)
package cis5550.jobs;

import java.util.*;
import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.kvs.KVSClient;

public class TitleIndexer {

    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("Starting Title Indexer (NO-POSITION version)...");

        KVSClient kvs = ctx.getKVS();

        // Step 1: load url + title
        FlameRDD crawlTitleRdd = ctx.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            String title = row.get("title");

            if (url == null || title == null)
                return null;

            return url + "|||appender|||" + title;
        });

        long titleProcessed = crawlTitleRdd.count();
        ctx.output("Total titles processed = " + titleProcessed);

        FlamePairRDD urlTitlePairs = crawlTitleRdd.mapToPair(line -> {
            int pos = line.indexOf("|||appender|||");
            if (pos < 0) return null;

            return new FlamePair(
                line.substring(0, pos),
                line.substring(pos + 14)
            );
        });

        // Step 2: title words (NO POSITION)
        FlamePairRDD wordUrlPairs = urlTitlePairs.flatMapToPair(pair -> {
            List<FlamePair> out = new ArrayList<>();
            String url = pair._1();
            String title = pair._2();

            String[] words = cleanupTitle(title);

            // unique words per title
            Set<String> uniq = new HashSet<>(Arrays.asList(words));

            for (String w : uniq) {
                out.add(new FlamePair(w, url));
            }
            return out;
        });

        // Step 3: fold word 
        FlamePairRDD folded = wordUrlPairs.foldByKey("", (acc, url) -> {
            if (acc.isEmpty()) return url;
            List<String> parts = Arrays.asList(acc.split(","));
            if (parts.contains(url)) return acc;
            return acc + "," + url;
        });

        // Step 4: Save using hex key + columns
        List<FlamePair> finalRows = folded.collect();

        for (FlamePair p : finalRows) {
            String word = p._1();
            String urls = p._2();

            String rowKey = Hasher.hash(word);

            kvs.put("pt-title-index", rowKey, "token", word);
            kvs.put("pt-title-index", rowKey, "urls", urls);
        }

        ctx.output("Title Index（Column） saved rows = " + kvs.count("pt-title-index"));
    }


    private static String[] cleanupTitle(String title) {
        if (title == null || title.isEmpty())
            return new String[0];

        title = title.toLowerCase();
        title = title.replaceAll("&[a-zA-Z]+;", " ");
        title = title.replaceAll("[^a-z0-9]+", " ").trim();

        String[] tokens = title.split("\\s+");
        List<String> cleaned = new ArrayList<>();

        for (String t : tokens) {
            if (t.isEmpty()) continue;
            if (!t.matches("[a-z][a-z0-9]*")) continue;
            if (t.matches("\\d+")) continue;
            if (t.length() > 30) continue;

            cleaned.add(t);
        }

        return cleaned.toArray(new String[0]);
    }
}
