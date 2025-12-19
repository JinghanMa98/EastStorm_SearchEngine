package cis5550.jobs;

import static cis5550.webserver.Server.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import java.util.*;
import java.util.stream.Collectors;

public class SearchServer {

    static final String COORD = "localhost:8000";
    
    // Weights for ranking
    static final double WEIGHT_TITLE = 1.5;
    static final double WEIGHT_URL = 1.0;
    static final double WEIGHT_PAGERANK = 5000.0; // Scaled up to match text score magnitude
    static final double TOTAL_DOCS_ESTIMATE = 30000.0; // Adjust the crawl count

    public static void main(String[] args) {

        // port(8080);
        securePort(443, "/home/ec2-user/keystore.jks", "secret");


        get("/", (req, res) -> 
        "<!DOCTYPE html>" +
        "<html lang='en'>" +
        "<head>" +
        "<meta charset='UTF-8' />" +
        "<meta name='viewport' content='width=device-width, initial-scale=1.0' />" +
        "<title>Boer Search</title>" +
        "<style>" +

        "body { " +
        "  margin: 0; padding: 0; " +
        "  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;" +
        "  background: #fafafa;" +
        "  display: flex; flex-direction: column;" +
        "  justify-content: center; align-items: center;" +
        "  height: 100vh;" +
        "  color: #111;" +
        "}" +

        ".title {" +
        "  font-size: 42px;" +
        "  font-weight: 600;" +
        "  margin-bottom: 40px;" +
        "  letter-spacing: -0.5px;" +
        "}" +

        "form { display: flex; align-items: center; }" +

        ".search-box {" +
        "  width: 320px;" +
        "  height: 42px;" +
        "  padding: 0 14px;" +
        "  border-radius: 20px;" +
        "  border: 1px solid #d0d0d0;" +
        "  background: #fff;" +
        "  font-size: 16px;" +
        "  outline: none;" +
        "  transition: all 0.2s ease;" +
        "  box-shadow: 0 2px 6px rgba(0,0,0,0.05);" +
        "}" +

        ".search-box:focus {" +
        "  border-color: #aaa;" +
        "  box-shadow: 0 4px 10px rgba(0,0,0,0.1);" +
        "}" +

        ".search-btn {" +
        "  height: 42px;" +
        "  margin-left: 12px;" +
        "  padding: 0 20px;" +
        "  border-radius: 20px;" +
        "  background: #000;" +
        "  color: #fff;" +
        "  border: none;" +
        "  font-size: 15px;" +
        "  cursor: pointer;" +
        "  transition: background 0.2s ease;" +
        "}" +

        ".search-btn:hover {" +
        "  background: #333;" +
        "}" +

        "</style>" +
        "</head>" +
        "<body>" +

        "<div class='title'>Eaststorm Search</div>" +

        "<form action='/search' method='get'>" +
          "<input class='search-box' type='text' name='q' placeholder='Search…' />" +
          "<button class='search-btn'>Go</button>" +
        "</form>" +

        "</body></html>"
    );


        get("/search", (req, res) -> {
            String q = req.queryParams("q");
            if (q == null || q.isBlank()) return "No query";

            try {
                String[] terms = q.toLowerCase().split("\\s+");
                Map<String, Double> urlScores = new HashMap<>();
                Map<String, Map<String, Double>> debugScores = new HashMap<>();
                KVSClient kvs = new KVSClient(COORD);
                
                for (String term : terms) {
                    String hashedTerm = Hasher.hash(term);
                    
                    // 1. Title Index
                    try {
                        Row row = kvs.getRow("pt-title-index", hashedTerm);
                        if (row != null) {
                            String urls = row.get("urls");
                            if (urls != null) {
                                String[] urlList = urls.split(",");
                                double idf = Math.log(TOTAL_DOCS_ESTIMATE / (1.0 + urlList.length));
                                for (String url : urlList) {
                                    double score = WEIGHT_TITLE * idf;
                                    urlScores.merge(url, score, Double::sum);
                                    debugScores.computeIfAbsent(url, k -> new HashMap<>()).merge("Title", score, Double::sum);
                                }
                            }
                        }
                    } catch (Exception e) {}
                    
                    // 2. URL Index
                    try {
                        Row row = kvs.getRow("pt-url-index", hashedTerm);
                        if (row != null) {
                            String urls = row.get("urls");
                            if (urls != null) {
                                String[] urlList = urls.split(",");
                                double idf = Math.log(TOTAL_DOCS_ESTIMATE / (1.0 + urlList.length));
                                for (String url : urlList) {
                                    double score = WEIGHT_URL * idf;
                                    urlScores.merge(url, score, Double::sum);
                                    debugScores.computeIfAbsent(url, k -> new HashMap<>()).merge("URL", score, Double::sum);
                                }
                            }
                        }
                    } catch (Exception e) {}
                    
                    // 3. Image Alt Index
                    // Removed

                    // 4. Body Index (pt-index)
                    // Removed
                }
                
                // 5. PageRank
                for (String url : urlScores.keySet()) {
                    try {
                        java.net.URI uri = new java.net.URI(url);
                        String host = uri.getHost();
                        if (host != null) {
                            host = host.toLowerCase();
                            Row row = kvs.getRow("pt-pageranks", host);
                            if (row != null) {
                                for (String col : row.columns()) {
                                    String val = row.get(col);
                                    try {
                                        double rank = Double.parseDouble(val);
                                        double score = rank * WEIGHT_PAGERANK;
                                        urlScores.merge(url, score, Double::sum);
                                        debugScores.computeIfAbsent(url, k -> new HashMap<>()).put("PageRank", score);
                                        break; 
                                    } catch (NumberFormatException e) {
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {}
                }
                
                // Sort and Top 50
                List<Map.Entry<String, Double>> sorted = urlScores.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(60)
                    .collect(Collectors.toList());

                List<Map.Entry<String, Double>> finalResults = new ArrayList<>();
                Map<String, String> urlTitles = new HashMap<>();

                for (Map.Entry<String, Double> entry : sorted) {
                    String url = entry.getKey();
                    double score = entry.getValue();
                    String title = url;

                    try {
                        String hashedUrl = Hasher.hash(url);
                        Row row = kvs.getRow("pt-crawl", hashedUrl);
                        if (row != null) {
                            String t = row.get("title");
                            if (t != null && !t.isBlank()) {
                                title = t;
                            }
                        }
                    } catch (Exception e) {}
                    
                    urlTitles.put(url, title);

                    String queryLower = q.trim().toLowerCase();
                    String titleLower = title.toLowerCase();
                    
                    if (titleLower.equals(queryLower)) {
                        double boost = 50.0;
                        score += boost;
                        debugScores.computeIfAbsent(url, k -> new HashMap<>()).put("TitleMatch", boost);
                    } else if (titleLower.startsWith(queryLower)) {
                        double boost = 25.0;
                        score += boost;
                        debugScores.computeIfAbsent(url, k -> new HashMap<>()).put("TitleMatch", boost);
                    }
                    
                    finalResults.add(new AbstractMap.SimpleEntry<>(url, score));
                }

                finalResults.sort(Map.Entry.<String, Double>comparingByValue().reversed());
                List<Map.Entry<String, Double>> displayList = finalResults.stream().limit(50).collect(Collectors.toList());
                    
                StringBuilder sb = new StringBuilder();
                sb.append("<!DOCTYPE html><html><head><title>Results for ").append(q).append("</title>");
                sb.append("<style>");
                sb.append("body { font-family: sans-serif; max-width: 800px; margin: 20px auto; padding: 0 20px; }");
                sb.append(".result { margin-bottom: 20px; }");
                sb.append(".title { font-size: 18px; margin-bottom: 4px; }");
                sb.append(".title a { text-decoration: none; color: #1a0dab; }");
                sb.append(".title a:hover { text-decoration: underline; }");
                sb.append(".url { color: #006621; font-size: 14px; }");
                sb.append(".score { color: #666; font-size: 12px; }");
                sb.append(".debug { font-size: 11px; color: #888; margin-top: 2px; }");
                sb.append("</style></head><body>");
                
                sb.append("<h3>Results for: ").append(q).append("</h3>");
                
                if (displayList.isEmpty()) {
                    sb.append("<p>No results found.</p>");
                } else {
                    for (Map.Entry<String, Double> entry : displayList) {
                        String url = entry.getKey();
                        double score = entry.getValue();
                        String title = urlTitles.getOrDefault(url, url);
                        
                        sb.append("<div class='result'>");
                        sb.append("<div class='title'><a href='").append(url).append("'>").append(title).append("</a></div>");
                        sb.append("<div class='url'>").append(url).append("</div>");
                        
                        sb.append("</div>");
                    }
                }
                sb.append("</body></html>");
                
                return sb.toString();

            } catch (Exception e) {
                e.printStackTrace();
                return "Error: " + e;
            }
        });
    }
}