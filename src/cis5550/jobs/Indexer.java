// Vivian Final Column-Based Safe Indexer (with Main Content Extraction + Debug Output)
package cis5550.jobs;

import java.util.*;
import java.util.regex.*;
import java.io.FileWriter;

import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.kvs.KVSClient;

public class Indexer {

    public static void run(FlameContext ctx, String[] args) throws Exception {

        KVSClient kvs = ctx.getKVS();

        // ===== Step 1: Load pt-crawl =====
        FlameRDD crawlDataRdd = ctx.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            String page = row.get("page");
            if (url == null || page == null)
                return null;
            return url + "|||appender|||" + page;
        });

        FlamePairRDD urlPagePairs = crawlDataRdd.mapToPair(line -> {
            int cut = line.indexOf("|||appender|||");
            if (cut < 0) return null;
            String u = line.substring(0, cut);
            String p = line.substring(cut + 14);
            return new FlamePair(u, p);
        });

        // ===== Step 2: Extract (word, url) pairs =====
        FlamePairRDD wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
            List<FlamePair> out = new ArrayList<>();
            String url = pair._1();
            String page = pair._2();

            // Vivian: use upgraded cleanup + main-content extraction
            String[] words = returnCleanedupPage(page);

            // unique words per page
            HashSet<String> uniq = new HashSet<>(Arrays.asList(words));

            for (String w : uniq) {
                out.add(new FlamePair(w, url));
            }

            return out;
        });

        // ===== Step 3: fold word → "url1,url2,..." =====
        FlamePairRDD index = wordUrlPairs.foldByKey("", (acc, url) -> {
            if (acc.isEmpty()) return url;
            List<String> lst = Arrays.asList(acc.split(","));
            if (lst.contains(url)) return acc;
            return acc + "," + url;
        });

        // =============================================
        // ===== Step 4: SAVE USING REAL COLUMNS ========
        // =============================================
        List<FlamePair> finalList = index.collect();

        for (FlamePair p : finalList) {
            String token = p._1();
            String urls  = p._2();

            String rowKey = Hasher.hash(token);   // SAFE rowKey

            kvs.put("pt-page-index", rowKey, "token", token);
            kvs.put("pt-page-index", rowKey, "urls", urls);
        }

        ctx.output("Saved rows into pt-index (column) = " + kvs.count("pt-page-index"));
        ctx.output("Indexer finished.");
    }


    // ===== Clean-up logic (Upgraded with Main Content Extraction + Debug Output) =====
    private static String[] returnCleanedupPage(String html) {
        if (html == null)
            return new String[0];

        String original = html;

        // ===== Step 1: remove script/style =====
        html = html.replaceAll("(?is)<script.*?>.*?</script>", " ");
        html = html.replaceAll("(?is)<style.*?>.*?</style>", " ");

        // ===== Step 2: extract <p>, <div>, <article>, <section> blocks =====
        Matcher m = Pattern.compile("(?is)<(p|div|article|section)[^>]*>(.*?)</\\1>").matcher(html);

        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String block = m.group(2)
                    .replaceAll("<[^>]+>", " ")     // strip inner tags
                    .replaceAll("\\s+", " ")        // normalize
                    .trim();

            if (block.length() > 50) {              // keep likely paragraphs
                sb.append(block).append("\n");
            }
        }

        // ===== Step 3: fallback if extracted too little =====
        String text = sb.toString().trim();
        if (text.length() < 80) {
            text = html.replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").trim();
        }

        // ===== Step 4: normalize =====
        text = text.toLowerCase();
        text = text.replaceAll("https?://\\S+", " ");
        text = text.replaceAll("[^a-z0-9]+", " ").trim();

        // ===== Step 5: DEBUG OUTPUT (append cleaned text into a file) =====
        try (FileWriter fw = new FileWriter("indexer_debug/indexer_extraction_log.txt", true)) {
            fw.write("====== NEW PAGE CLEANED RESULT ======\n");
            fw.write(text + "\n\n");
            System.out.println("Write files");

        } catch (Exception e) {
            System.out.println("Write debug file error: " + e.getMessage());
        }

        // ===== Step 6: tokenize =====
        String[] tokens = text.split("\\s+");

        List<String> out = new ArrayList<>();
        for (String t : tokens) {
            if (t.isEmpty()) continue;
            if (!t.matches("[a-z]+")) continue;
            if (t.length() > 30) continue;
            out.add(t);
        }

        return out.toArray(new String[0]);
    }
}
