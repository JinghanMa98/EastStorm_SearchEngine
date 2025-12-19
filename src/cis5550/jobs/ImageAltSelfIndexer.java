// Vivian Final Column-Based Safe ImageAltSelfIndexer
package cis5550.jobs;

import java.util.*;
import java.util.regex.*;
import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.kvs.KVSClient;

public class ImageAltSelfIndexer {

    private static final Set<String> USELESS_ALTS = new HashSet<>(Arrays.asList(
        "logo", "icon", "image", "img", "photo", "picture",
        "banner", "thumbnail", "thumb", "avatar", "spacer",
        "header", "footer", "advertisement", "ad", "promo",
        "loading", "placeholder", "default", "profile", "user"
    ));

    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("Starting Clean ImageAltSelfIndexer (column version)...");

        KVSClient kvs = ctx.getKVS();

        // ===== Step 1: Load crawl data =====
        FlameRDD crawlDataRdd = ctx.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            String page = row.get("page");
            if (url == null || page == null)
                return null;
            return url + "|||appender|||" + page;
        });

        FlamePairRDD urlPagePairs = crawlDataRdd.mapToPair(line -> {
            int pos = line.indexOf("|||appender|||");
            if (pos < 0) return null;

            return new FlamePair(
                line.substring(0, pos),
                line.substring(pos + 14)
            );
        });

        // ===== Step 2: Extract meaningful alt words =====
        FlamePairRDD wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
            List<FlamePair> out = new ArrayList<>();

            String pageUrl = pair._1();
            String page = pair._2();

            if (page == null || page.isEmpty()) return out;

            Pattern p = Pattern.compile("(?i)<img[^>]*alt\\s*=\\s*\"([^\"]*)\"");
            Matcher m = p.matcher(page);

            while (m.find()) {
                String alt = m.group(1).trim();
                if (alt.isEmpty()) continue;

                String altLower = alt.toLowerCase();
                if (altLower.length() <= 3) continue;
                if (USELESS_ALTS.contains(altLower)) continue;
                if (altLower.matches("^[0-9]+$")) continue;

                String[] words = cleanupMeaningfulAlt(altLower);

                for (String w : words) {
                    if (!w.isEmpty())
                        out.add(new FlamePair(w, pageUrl));
                }
            }

            return out;
        });

        // ===== Step 3: fold by word → "url1,url2,..." =====
        FlamePairRDD folded = wordUrlPairs.foldByKey("", (a, b) -> {
            if (a.isEmpty()) return b;

            List<String> existing = Arrays.asList(a.split(","));
            if (existing.contains(b))
                return a;

            return a + "," + b;
        });

        // =============================================
        // ===== Step 4: Column-based save (hex key) ====
        // =============================================
        List<FlamePair> finalRows = folded.collect();

        for (FlamePair p : finalRows) {
            String word = p._1();
            String urls = p._2();

            String rowKey = Hasher.hash(word);  // SAFE hex key

            kvs.put("pt-imagealt-index", rowKey, "token", word);
            kvs.put("pt-imagealt-index", rowKey, "urls", urls);
        }

        ctx.output("Clean alt-index saved (column), rows = " +
            kvs.count("pt-imagealt-index"));
    }


    // ================= ALT TOKEN CLEANUP ==========================
    private static String[] cleanupMeaningfulAlt(String text) {

        text = text.replaceAll("(?s)<[^>]*>", " ");
        text = text.replaceAll("[^a-zA-Z ]+", " ");
        text = text.toLowerCase().trim();

        String[] tokens = text.split("\\s+");
        List<String> cleaned = new ArrayList<>();

        for (String t : tokens) {
            if (t.length() == 0) continue;
            if (!t.matches("^[a-z]+$")) continue;
            if (USELESS_ALTS.contains(t)) continue;
            if (t.length() > 30) continue;

            cleaned.add(t);
        }
        return cleaned.toArray(new String[0]);
    }

}
