package cis5550.jobs;

import java.util.*;
import java.util.regex.*;
import cis5550.flame.*;

public class AnchorTextIndexer {

    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("Starting OutgoingAnchorIndexer...");

        int crawlCount = ctx.getKVS().count("pt-crawl");
        ctx.output("Found " + crawlCount + " rows in pt-crawl");

        // 1) load url + page
        FlameRDD crawlDataRdd = ctx.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            String page = row.get("page");

            if (url == null || page == null)
                return null;

            return url + "|||appender|||" + page;
        });

        long count = crawlDataRdd.count();
        ctx.output("Loaded " + count + " rows with pages");

        // 2) map to (url, page)
        FlamePairRDD urlPagePairs = crawlDataRdd.mapToPair(line -> {
            int pos = line.indexOf("|||appender|||");
            if (pos < 0) return null;

            String url = line.substring(0, pos);
            String page = line.substring(pos + 14);

            return new FlamePair(url, page);
        });

        // 3) extract <a href="">anchor text</a> and produce (word, targetUrl|||position|||k)
        FlamePairRDD wordPosPairs = urlPagePairs.flatMapToPair(pair -> {
            List<FlamePair> out = new ArrayList<>();

            String page = pair._2();
            if (page == null || page.isEmpty()) return out;

            // match <a href="target">text</a>
            Pattern p = Pattern.compile("(?i)<a\\s+[^>]*href\\s*=\\s*\"([^\"]+)\"[^>]*>(.*?)</a>");
            Matcher m = p.matcher(page);

            while (m.find()) {
                String targetUrl = m.group(1).trim();
                String anchor = m.group(2);

                String[] words = cleanupAnchor(anchor);

                for (int i = 0; i < words.length; i++) {
                    String w = words[i];
                    if (w == null || w.isEmpty()) continue;

                    // IMPORTANT: same format as TitleIndexer
                    out.add(new FlamePair(
                            w,
                            targetUrl + "|||position|||" + (i + 1)
                    ));
                }
            }

            return out;
        });

        // 4) fold to "url|||position|||x,url|||position|||y"
        FlamePairRDD folded = wordPosPairs.foldByKey("", (a, b) -> {
            if (a.isEmpty()) return b;
            return a + "," + b;
        });

        // 5) apply valueProcessing (same as Title/Page indexers)
        FlamePairRDD finalIndex = folded.flatMapToPair(pair -> {
            String word = pair._1();
            String raw = pair._2();
            String processed = valueProcessing(raw);

            List<FlamePair> r = new ArrayList<>();
            r.add(new FlamePair(word, processed));
            return r;
        });

        int finalCount = finalIndex.collect().size();
        ctx.output("Saving " + finalCount + " anchor words into pt-anchor-index");

        // 6) save as table
        finalIndex.saveAsTable("pt-anchor-index");

        int saved = ctx.getKVS().count("pt-anchor-index");
        ctx.output("OutgoingAnchorIndexer complete! Saved " + saved + " entries.");
    }

    // ======== CLEANUP ANCHOR TEXT ========
    private static String[] cleanupAnchor(String text) {
        if (text == null)
            return new String[0];

        // remove tags inside anchor (rare but possible)
        text = text.replaceAll("(?s)<[^>]*>", " ");
        text = text.toLowerCase();
        text = text.replaceAll("[^a-z0-9]+", " ").trim();

        String[] tokens = text.split("\\s+");

        List<String> cleaned = new ArrayList<>();
        for (String t : tokens) {
            if (t.isEmpty()) continue;
            if (!t.matches("[a-z][a-z0-9]*")) continue;
            if (t.length() > 30) continue;
            cleaned.add(t);
        }

        return cleaned.toArray(new String[0]);
    }

    // ======== SAME AS TITLE INDEXER ========
    private static String valueProcessing(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }

        // url => positions
        Map<String, TreeSet<Integer>> urlPositions = new HashMap<>();
        String[] entries = value.split(",");

        for (String entry : entries) {
            int sep = entry.indexOf("|||position|||");
            if (sep < 0) continue;

            String url = entry.substring(0, sep);
            int pos = Integer.parseInt(entry.substring(sep + 14).trim());

            urlPositions.computeIfAbsent(url, k -> new TreeSet<>()).add(pos);
        }

        // sort url by #position occurrences (desc)
        List<String> urls = new ArrayList<>(urlPositions.keySet());
        urls.sort((u1, u2) ->
                urlPositions.get(u2).size() - urlPositions.get(u1).size()
        );

        // build output string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < urls.size(); i++) {
            String url = urls.get(i);
            TreeSet<Integer> posSet = urlPositions.get(url);

            sb.append(url).append(":");

            int j = 0;
            for (Integer p : posSet) {
                sb.append(p);
                if (j < posSet.size() - 1)
                    sb.append(" ");
                j++;
            }

            if (i < urls.size() - 1)
                sb.append(",");
        }

        return sb.toString();
    }
}
