package cis5550.jobs;

import java.util.*;
import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;

public class PageRank {

    private static final double DECAY = 0.85;
    private static final int MAX_ITERATIONS = 20;
    private static final double CONVERGENCE_THRESHOLD = 0.0001;

    private static final Set<String> SOCIAL_MEDIA = new HashSet<>(Arrays.asList(
        "facebook.com","www.facebook.com",
        "twitter.com","www.twitter.com","x.com",
        "instagram.com","www.instagram.com",
        "youtube.com","www.youtube.com",
        "tiktok.com","www.tiktok.com",
        "linkedin.com","www.linkedin.com",
        "reddit.com","www.reddit.com",
        "pinterest.com","www.pinterest.com"
    ));

    private static boolean isWikiFamily(String host) {
        if (host == null) return false;
        host = host.toLowerCase();
        return host.endsWith(".wikipedia.org") ||
               host.endsWith(".wiktionary.org") ||
               host.endsWith(".wikibooks.org") ||
               host.endsWith(".wikiquote.org") ||
               host.endsWith(".wikinews.org") ||
               host.endsWith(".wikivoyage.org") ||
               host.endsWith(".wikiversity.org") ||
               host.endsWith(".wikidata.org") ||
               host.endsWith(".wikimedia.org");
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        System.out.println("Starting HOST-LEVEL PageRank...");

        ////////////////////////////////////////////////////////////////////////
        // 1. Build HOST  HOST link graph
        ////////////////////////////////////////////////////////////////////////

        FlamePairRDD stateTable = ctx.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            String page = row.get("page");
            if (url == null || page == null)
                return null;

            try {
                String host = new java.net.URI(url).getHost();
                if (host == null) return null;
                host = host.toLowerCase();

                List<String> links = extractLinks(page, url);

                Set<String> hostLinks = new HashSet<>();
                for (String link : links) {
                    try {
                        String h = new java.net.URI(link).getHost();
                        if (h != null) hostLinks.add(h.toLowerCase());
                    } catch (Exception ignore) {}
                }

                StringBuilder sb = new StringBuilder();
                sb.append(host).append(";");

                boolean first = true;
                for (String h : hostLinks) {
                    if (!first) sb.append(",");
                    sb.append(h);
                    first = false;
                }

                return sb.toString();

            } catch (Exception e) {
                return null;
            }
        })

        .mapToPair(s -> {
            if (s == null) return null;
            int semi = s.indexOf(';');
            if (semi == -1) return null;
            return new FlamePair(s.substring(0, semi), s.substring(semi + 1));
        })

        .foldByKey("", (a, b) -> {
            Set<String> out = new HashSet<>();
            if (a != null && !a.isEmpty())
                out.addAll(Arrays.asList(a.split(",")));
            if (b != null && !b.isEmpty())
                out.addAll(Arrays.asList(b.split(",")));
            out.remove("");
            return String.join(",", out);
        });

        ////////////////////////////////////////////////////////////////////////
        // 2. Count nodes
        ////////////////////////////////////////////////////////////////////////

        long totalHosts = stateTable.flatMap(x -> Arrays.asList("1")).count();
        ctx.output("Total unique hosts = " + totalHosts);

        if (totalHosts == 0) {
            ctx.output("ERROR: No hosts found.");
            return;
        }

        ////////////////////////////////////////////////////////////////////////
        // 3. Initialize ranks
        ////////////////////////////////////////////////////////////////////////

        final double initialRank = 1.0 / totalHosts;

        FlamePairRDD ranks = stateTable.flatMapToPair(p ->
            Collections.singletonList(new FlamePair(p._1(), String.valueOf(initialRank)))
        );

        ctx.output("Initialized rank = " + initialRank);

        FlamePairRDD oldRanks = ranks;

        ////////////////////////////////////////////////////////////////////////
        // 4. PageRank iterations
        ////////////////////////////////////////////////////////////////////////

        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            ctx.output("Iteration " + (iter + 1));

            FlamePairRDD joined = stateTable.join(ranks);

            ////////////////////////////////////////////////////////////////////
            // Dangling nodes
            ////////////////////////////////////////////////////////////////////

            FlamePairRDD dangling = joined.flatMapToPair(p -> {
                String combined = p._2();
                int idx = combined.lastIndexOf(',');

                String links = (idx == -1 ? "" : combined.substring(0, idx));
                String rankStr = (idx == -1 ? combined : combined.substring(idx + 1));

                double rank;
                try { rank = Double.parseDouble(rankStr); }
                catch (Exception e) { rank = initialRank; }

                List<FlamePair> out = new ArrayList<>();
                if (links.isEmpty())
                    out.add(new FlamePair("__DANGLING__", String.valueOf(rank)));

                return out;
            });

            double totalDanglingRank = 0.0;

            List<FlamePair> dlist = dangling
                .foldByKey("0.0", (a, b) -> "" + (Double.parseDouble(a) + Double.parseDouble(b)))
                .collect();

            if (!dlist.isEmpty())
                totalDanglingRank = Double.parseDouble(dlist.get(0)._2());

            final double danglingContribution = totalDanglingRank / totalHosts;

            ////////////////////////////////////////////////////////////////////
            // Contributions
            ////////////////////////////////////////////////////////////////////

            FlamePairRDD contrib = joined.flatMapToPair(p -> {
                String host = p._1();
                String combined = p._2();

                int idx = combined.lastIndexOf(',');
                String links = (idx == -1 ? "" : combined.substring(0, idx));
                String rankStr = (idx == -1 ? combined : combined.substring(idx + 1));

                double rank;
                try { rank = Double.parseDouble(rankStr); }
                catch (Exception e) { rank = initialRank; }

                List<FlamePair> out = new ArrayList<>();
                out.add(new FlamePair(host, "0.0"));

                if (!links.isEmpty()) {
                    String[] arr = links.split(",");
                    double c = rank / arr.length;
                    for (String h : arr)
                        if (!h.isEmpty())
                            out.add(new FlamePair(h, "" + c));
                }
                return out;
            });

            FlamePairRDD aggregated = contrib.foldByKey("0.0",
                (a, b) -> "" + (Double.parseDouble(a) + Double.parseDouble(b)));

            ranks = aggregated.flatMapToPair(kv -> {
                double incoming = Double.parseDouble(kv._2());
                double newRank = (1 - DECAY) / totalHosts +
                                 DECAY * (incoming + danglingContribution);
                return Collections.singletonList(
                    new FlamePair(kv._1(), String.format("%.8f", newRank)));
            });

            ////////////////////////////////////////////////////////////////////
            // Convergence check
            ////////////////////////////////////////////////////////////////////

            if (iter > 0) {
                FlamePairRDD diff = oldRanks.join(ranks)
                    .flatMapToPair(p -> {
                        String comb = p._2();
                        int idx = comb.lastIndexOf(',');
                        double oldV = Double.parseDouble(comb.substring(0, idx));
                        double newV = Double.parseDouble(comb.substring(idx + 1));
                        return Collections.singletonList(
                            new FlamePair("__DIFF__", "" + Math.abs(newV - oldV)));
                    });

                double totalDiff = Double.parseDouble(
                    diff.foldByKey("0.0",
                        (a, b) -> "" + (Double.parseDouble(a) + Double.parseDouble(b))
                    ).collect().get(0)._2()
                );

                if (totalDiff < CONVERGENCE_THRESHOLD * totalHosts) {
                    ctx.output("Converged at iteration " + (iter + 1));
                    break;
                }
            }

            oldRanks = ranks;
        }

        ////////////////////////////////////////////////////////////////////////
        // 5. Save + Social Media DownWeight + Wiki Merge
        ////////////////////////////////////////////////////////////////////////

        ranks.saveAsTable("pt-pageranks");
        ctx.output("Saved to pt-pageranks");

        List<FlamePair> all = ranks.collect();

        double wikiSum = 0.0;
        Map<String, Double> finalScores = new HashMap<>();

        for (FlamePair p : all) {
            String host = p._1().toLowerCase();
            double v = Double.parseDouble(p._2());

            // Social Media ↓↓↓
            if (SOCIAL_MEDIA.contains(host))
                v *= 0.10;

            // Wiki family ↓↓↓
            if (isWikiFamily(host))
                wikiSum += v;
            else
                finalScores.put(host, v);
        }

        // Add merged WikiFamily
        finalScores.put("WikiFamily(all)", wikiSum);

        List<Map.Entry<String, Double>> sorted = new ArrayList<>(finalScores.entrySet());
        sorted.sort((a, b) -> -Double.compare(a.getValue(), b.getValue()));

        ctx.output("Top 1000 hosts (Final Ranking):");
        for (int i = 0; i < Math.min(1000, sorted.size()); i++) {
            ctx.output((i + 1) + ". " +
                sorted.get(i).getKey() +
                " = " + String.format("%.8f", sorted.get(i).getValue()));
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // LINK EXTRACTION 
    ///////////////////////////////////////////////////////////////////////////

private static final int MAX_LINKS = 500;  

private static List<String> extractLinks(String html, String baseUrl) {
    List<String> links = new ArrayList<>();
    if (html == null || html.length() == 0) return links;

    int len = html.length();
    int i = 0;

    while (i < len) {

        if (links.size() >= MAX_LINKS)
            break;

            if (html.regionMatches(true, i, "<a", 0, 2)) {
                int tagStart = i;
                int tagEnd = html.indexOf(">", tagStart);
                if (tagEnd == -1) break;

                String tag = html.substring(tagStart, tagEnd + 1);

                extractAttribute(tag, "href", baseUrl, links);

                i = tagEnd + 1;
            } else {
            i++;
        }
    }

    return links;
}


    private static void extractAttribute(String tag, String attribute, String baseUrl, List<String> out) {
        String patternStr =
            attribute + "\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^>\\s]+))";
        java.util.regex.Matcher m =
            java.util.regex.Pattern.compile(patternStr,
                java.util.regex.Pattern.CASE_INSENSITIVE).matcher(tag);

        if (m.find()) {
            String u = m.group(1);
            if (u == null) u = m.group(2);
            if (u == null) u = m.group(3);

            if (u != null) {
                String norm = normalizeUrl(u, baseUrl);
                if (norm != null) out.add(norm);
            }
        }
    }

    private static String normalizeUrl(String href, String baseUrl) {
        try {
            if (href.startsWith("http")) return cleanUrl(href);
            if (href.startsWith("//")) return cleanUrl("http:" + href);

            if (href.startsWith("/")) {
                java.net.URI base = new java.net.URI(baseUrl);
                return cleanUrl(base.getScheme() + "://" + base.getHost() + href);
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String cleanUrl(String url) {
        int idx = url.indexOf('#');
        return (idx == -1) ? url : url.substring(0, idx);
    }
}
