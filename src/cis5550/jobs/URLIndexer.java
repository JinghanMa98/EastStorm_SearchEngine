// Vivian Final Column-Based Safe URLIndexer
package cis5550.jobs;

import java.util.*;
import java.net.URI;
import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.kvs.KVSClient;

public class URLIndexer {

    private static final Set<String> USELESS_TOKENS = new HashSet<>(Arrays.asList(
        "cgi", "action", "id", "page", "view", "list", "item", "detail",
        "login", "logout", "register", "signup", "signin",
        "search", "query", "q", "ref", "utm", "src", "sid", "session", "sessionid",
        "token", "auth", "verify", "confirm",
        "rss", "feed", "sitemap",
        "xml", "json", "txt", "pdf", "csv",
        "img", "image", "images",
        "jpg", "jpeg", "png", "gif", "svg", "ico",
        "css", "js",
        "mobile", "m", "amp", "api",
        "static", "assets", "content",
        "temp", "tmp", "backup", "old", "new",
        "error", "debug", "404", "500"
    ));

    public static void run(FlameContext ctx, String[] args) throws Exception {

        ctx.output("Starting URLIndexer (column-based)...");

        KVSClient kvs = ctx.getKVS();

        // Step 1: read all URLs
        FlameRDD urlRdd = ctx.fromTable("pt-crawl", row -> row.get("url"));

        // Step 2: URL (token, url)
        FlamePairRDD tokenUrlPairs = urlRdd.flatMapToPair(urlStr -> {
            List<FlamePair> out = new ArrayList<>();

            if (urlStr == null || urlStr.isEmpty()) return out;

            Set<String> tokens = extractTokensFromUrl(urlStr);

            for (String t : tokens)
                out.add(new FlamePair(t, urlStr));

            return out;
        });

        // Step 3: fold token url1,url2,url3
        FlamePairRDD folded = tokenUrlPairs.foldByKey("", (acc, url) -> {
            if (acc.isEmpty()) return url;

            for (String s : acc.split(",")) {
                if (s.equals(url)) return acc;   // dedupe
            }

            return acc + "," + url;
        });

        // Step 4: Column-based KVS save (hex rowKey)
        List<FlamePair> finalRows = folded.collect();

        for (FlamePair p : finalRows) {
            String token = p._1();
            String urls = p._2();

            String rowKey = Hasher.hash(token);   // SAFE row key

            kvs.put("pt-url-index", rowKey, "token", token);
            kvs.put("pt-url-index", rowKey, "urls", urls);
        }

        ctx.output("URLIndexer（columns） saved rows = " + kvs.count("pt-url-index"));
    }


    // Extract meaningful tokens from URL path
    private static Set<String> extractTokensFromUrl(String rawUrl) {
        Set<String> result = new HashSet<>();
        if (rawUrl == null) return result;

        try {
            URI uri = new URI(rawUrl);
            String path = uri.getPath();

            if (path == null || path.isEmpty()) return result;

            path = path.toLowerCase().replaceAll("[^a-z]+", " ").trim();
            if (path.isEmpty()) return result;

            String[] tokens = path.split("\\s+");

            for (String t : tokens) {
                if (t.length() < 2) continue;
                if (t.length() > 30) continue;
                if (!t.matches("^[a-z]+$")) continue;
                if (USELESS_TOKENS.contains(t)) continue;

                result.add(t);
            }

        } catch (Exception ignored) {}

        return result;
    }
}
