package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.URLParser;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Analyzes the quality of crawled content from pt-crawl table.
 * Checks host distribution, content quality, language, and other metrics.
 * 
 * Can be run as a Flame job (recommended) or standalone program.
 */
public class CrawlQualityAnalyzer {
    
    /**
     * Flame job entry point - recommended way to run the analyzer
     */
    public static void run(FlameContext ctx, String[] args) throws Exception {
        int sampleSize = args.length > 0 ? Integer.parseInt(args[0]) : 10000;
        
        KVSClient kvs = ctx.getKVS();
        
        ctx.output("=".repeat(80));
        ctx.output("CRAWL QUALITY ANALYSIS");
        ctx.output("=".repeat(80));
        ctx.output("");
        
        // Get total count
        int totalCount = kvs.count("pt-crawl");
        ctx.output("Total pages crawled: " + totalCount);
        ctx.output("");
        
        // Sample pages for analysis
        ctx.output("Sampling pages for analysis...");
        List<Row> sample = samplePages(kvs, totalCount, sampleSize, ctx);
        ctx.output("Analyzing " + sample.size() + " pages");
        ctx.output("");
        
        // Run all analyses
        analyzeHostDistribution(sample, ctx);
        analyzeResponseCodes(sample, ctx);
        analyzeContentTypes(sample, ctx);
        analyzePageSizes(sample, ctx);
        analyzeLanguage(sample, ctx);
        analyzeDomainDiversity(sample, ctx);
        analyzeURLPatterns(sample, ctx);
        analyzeContentQuality(sample, ctx);
        
        ctx.output("=".repeat(80));
        ctx.output("ANALYSIS COMPLETE");
        ctx.output("=".repeat(80));
    }
    
    /**
     * Standalone program entry point - for testing/debugging
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage (standalone): java cis5550.jobs.CrawlQualityAnalyzer <coordinator> [sampleSize]");
            System.err.println("Usage (Flame job):   java -cp \"lib/*:bin\" cis5550.flame.FlameSubmit localhost:9000 jobs.jar cis5550.jobs.CrawlQualityAnalyzer [sampleSize]");
            System.err.println("");
            System.err.println("  coordinator: KVS coordinator address (e.g., localhost:8000)");
            System.err.println("  sampleSize:  Number of pages to sample (default: 10000, use 0 for all)");
            System.err.println("");
            System.err.println("NOTE: When running on EC2, use 'localhost:8000' not the public IP!");
            System.err.println("NOTE: Running as a Flame job is recommended for better reliability!");
            System.exit(1);
        }
        
        String coordinator = args[0];
        int sampleSize = args.length > 1 ? Integer.parseInt(args[1]) : 10000;
        
        System.out.println("Connecting to KVS coordinator at: " + coordinator);
        KVSClient kvs = new KVSClient(coordinator);
        System.out.println("Connected successfully");
        System.out.println();
        
        System.out.println("=".repeat(80));
        System.out.println("CRAWL QUALITY ANALYSIS");
        System.out.println("=".repeat(80));
        System.out.println();
        
        // Get total count
        int totalCount = kvs.count("pt-crawl");
        System.out.println("Total pages crawled: " + totalCount);
        System.out.println();
        
        // Sample pages for analysis
        System.out.println("Sampling pages for analysis...");
        List<Row> sample = samplePages(kvs, totalCount, sampleSize, null);
        System.out.println("Analyzing " + sample.size() + " pages");
        System.out.println();
        
        // 1. Host Distribution
        analyzeHostDistribution(sample, null);
        
        // 2. Response Code Distribution
        analyzeResponseCodes(sample, null);
        
        // 3. Content Type Distribution
        analyzeContentTypes(sample, null);
        
        // 4. Page Size Distribution
        analyzePageSizes(sample, null);
        
        // 5. Language Check (English content)
        analyzeLanguage(sample, null);
        
        // 6. Domain Diversity
        analyzeDomainDiversity(sample, null);
        
        // 7. URL Pattern Analysis
        analyzeURLPatterns(sample, null);
        
        // 8. Content Quality Indicators
        analyzeContentQuality(sample, null);
        
        System.out.println("=".repeat(80));
        System.out.println("ANALYSIS COMPLETE");
        System.out.println("=".repeat(80));
    }
    
    // Helper method to output text (works with both Flame and standalone)
    private static void output(FlameContext ctx, String text) {
        if (ctx != null) {
            ctx.output(text);
        } else {
            System.out.println(text);
        }
    }
    
    private static List<Row> samplePages(KVSClient kvs, int totalCount, int sampleSize, FlameContext ctx) throws Exception {
        List<Row> sample = new ArrayList<>();
        
        output(ctx, "Scanning pt-crawl table...");
        output(ctx, "  Attempting to create iterator...");
        
        Iterator<Row> it;
        try {
            it = kvs.scan("pt-crawl");
            output(ctx, "  Iterator created successfully");
        } catch (Exception e) {
            String msg = "ERROR: Failed to create iterator: " + e.getMessage();
            if (ctx != null) {
                ctx.output(msg);
            } else {
                System.err.println(msg);
                e.printStackTrace();
            }
            return sample;
        }
        
        // Test if iterator has any data
        output(ctx, "  Checking if iterator has data...");
        boolean hasData = it.hasNext();
        output(ctx, "  Iterator hasNext() = " + hasData);
        
        if (!hasData) {
            String msg = "WARNING: Iterator reports no data available!\n" +
                "This might mean:\n" +
                "  1. The table is empty (but count says " + totalCount + ")\n" +
                "  2. Network connectivity issue to KVS workers\n" +
                "  3. The /data/pt-crawl endpoint is not working\n" +
                "\n" +
                "Try testing manually:\n" +
                "  curl http://localhost:8001/data/pt-crawl | head -c 1000";
            if (ctx != null) {
                ctx.output(msg);
            } else {
                System.err.println(msg);
            }
            return sample;
        }
        
        if (sampleSize <= 0 || sampleSize >= totalCount) {
            // Sample all pages
            output(ctx, "Loading all " + totalCount + " pages...");
            int count = 0;
            int nullCount = 0;
            while (it.hasNext()) {
                Row row = it.next();
                if (row != null) {
                    sample.add(row);
                    count++;
                    if (count % 10000 == 0) {
                        output(ctx, "  Loaded " + count + " pages...");
                    }
                } else {
                    nullCount++;
                    if (nullCount < 10) {
                        output(ctx, "  WARNING: Got null row (count: " + nullCount + ")");
                    }
                }
            }
            output(ctx, "  Loaded " + count + " total pages (skipped " + nullCount + " null rows)");
        } else {
            // Systematic sampling (every Nth page) for efficiency
            output(ctx, "Taking systematic sample of " + sampleSize + " pages from " + totalCount + " total...");
            int step = Math.max(1, totalCount / sampleSize);
            output(ctx, "  Sampling every " + step + " pages...");
            
            int index = 0;
            int sampled = 0;
            int processed = 0;
            int nullCount = 0;
            
            while (it.hasNext() && sampled < sampleSize) {
                Row row = it.next();
                if (row != null) {
                    processed++;
                    // Take every Nth page
                    if (index % step == 0) {
                        sample.add(row);
                        sampled++;
                    }
                    index++;
                    
                    if (processed % 10000 == 0) {
                        output(ctx, "  Processed " + processed + " pages, sampled " + sampled + "...");
                    }
                } else {
                    // Skip null rows but don't increment index
                    processed++;
                    nullCount++;
                    if (nullCount < 10) {
                        output(ctx, "  WARNING: Got null row (count: " + nullCount + ")");
                    }
                }
            }
            output(ctx, "  Processed " + processed + " pages, sampled " + sampled + " pages (skipped " + nullCount + " null rows)");
        }
        
        if (sample.isEmpty()) {
            String msg = "\nERROR: No pages sampled despite count showing " + totalCount + " pages!\n" +
                "This suggests a problem with the scan iterator.";
            if (ctx != null) {
                ctx.output(msg);
            } else {
                System.err.println(msg);
            }
        }
        
        return sample;
    }
    
    private static void analyzeHostDistribution(List<Row> sample, FlameContext ctx) {
        output(ctx, "1. HOST DISTRIBUTION");
        output(ctx, "-".repeat(80));
        
        Map<String, Integer> hostCounts = new HashMap<>();
        for (Row row : sample) {
            String url = row.get("url");
            if (url != null) {
                try {
                    String[] parsed = URLParser.parseURL(url);
                    String host = parsed[1]; // host
                    if (host != null) {
                        host = host.toLowerCase();
                        hostCounts.put(host, hostCounts.getOrDefault(host, 0) + 1);
                    }
                } catch (Exception e) {
                    // Skip invalid URLs
                }
            }
        }
        
        // Sort by count
        List<Map.Entry<String, Integer>> sorted = hostCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(Collectors.toList());
        
        output(ctx, "Total unique hosts: " + hostCounts.size());
        output(ctx, "");
        output(ctx, "Top 20 hosts by page count:");
        String header = String.format("%-50s %10s %8s", "Host", "Pages", "%");
        output(ctx, header);
        output(ctx, "-".repeat(80));
        
        int total = sample.size();
        for (int i = 0; i < Math.min(20, sorted.size()); i++) {
            Map.Entry<String, Integer> entry = sorted.get(i);
            double percentage = (entry.getValue() * 100.0) / total;
            String formatted = String.format("%-50s %10d %7.2f%%", 
                truncate(entry.getKey(), 50), entry.getValue(), percentage);
            output(ctx, formatted);
        }
        
        // Check if too concentrated
        if (sorted.size() > 0) {
            int top10Count = sorted.stream().limit(10).mapToInt(Map.Entry::getValue).sum();
            double top10Percent = (top10Count * 100.0) / total;
            output(ctx, "");
            output(ctx, "Top 10 hosts account for: " + String.format("%.2f", top10Percent) + "% of pages");
            if (top10Percent > 80) {
                output(ctx, "⚠️  WARNING: Very concentrated - consider adding more diverse seed URLs");
            } else if (top10Percent > 60) {
                output(ctx, "⚠️  CAUTION: Somewhat concentrated - good diversity but could be better");
            } else {
                output(ctx, "✅ GOOD: Well distributed across hosts");
            }
        }
        output(ctx, "");
    }
    
    private static void analyzeResponseCodes(List<Row> sample, FlameContext ctx) {
        output(ctx, "2. RESPONSE CODE DISTRIBUTION");
        output(ctx, "-".repeat(80));
        
        Map<String, Integer> codeCounts = new HashMap<>();
        for (Row row : sample) {
            String code = row.get("responseCode");
            if (code != null) {
                codeCounts.put(code, codeCounts.getOrDefault(code, 0) + 1);
            }
        }
        
        String header = String.format("%-15s %10s %8s", "Response Code", "Count", "%");
        output(ctx, header);
        output(ctx, "-".repeat(80));
        
        int total = sample.size();
        List<Map.Entry<String, Integer>> sorted = codeCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(Collectors.toList());
        
        for (Map.Entry<String, Integer> entry : sorted) {
            double percentage = (entry.getValue() * 100.0) / total;
            String formatted = String.format("%-15s %10d %7.2f%%", entry.getKey(), entry.getValue(), percentage);
            output(ctx, formatted);
        }
        
        // Check success rate
        int successCount = codeCounts.getOrDefault("200", 0);
        double successRate = (successCount * 100.0) / total;
        output(ctx, "");
        output(ctx, "Success rate (200 OK): " + String.format("%.2f", successRate) + "%");
        if (successRate < 70) {
            output(ctx, "⚠️  WARNING: Low success rate - many pages failed to crawl");
        } else if (successRate < 85) {
            output(ctx, "⚠️  CAUTION: Moderate success rate");
        } else {
            output(ctx, "✅ GOOD: High success rate");
        }
        output(ctx, "");
    }
    
    private static void analyzeContentTypes(List<Row> sample, FlameContext ctx) {
        output(ctx, "3. CONTENT TYPE DISTRIBUTION");
        output(ctx, "-".repeat(80));
        
        Map<String, Integer> typeCounts = new HashMap<>();
        for (Row row : sample) {
            String type = row.get("contentType");
            if (type != null) {
                // Simplify content type (remove charset, etc.)
                if (type.contains(";")) {
                    type = type.split(";")[0].trim();
                }
                typeCounts.put(type, typeCounts.getOrDefault(type, 0) + 1);
            }
        }
        
        String header = String.format("%-40s %10s %8s", "Content Type", "Count", "%");
        output(ctx, header);
        output(ctx, "-".repeat(80));
        
        int total = sample.size();
        List<Map.Entry<String, Integer>> sorted = typeCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(Collectors.toList());
        
        for (Map.Entry<String, Integer> entry : sorted) {
            double percentage = (entry.getValue() * 100.0) / total;
            String formatted = String.format("%-40s %10d %7.2f%%", 
                truncate(entry.getKey(), 40), entry.getValue(), percentage);
            output(ctx, formatted);
        }
        
        // Check HTML content
        int htmlCount = typeCounts.getOrDefault("text/html", 0);
        double htmlPercent = (htmlCount * 100.0) / total;
        output(ctx, "");
        output(ctx, "HTML content (text/html): " + String.format("%.2f", htmlPercent) + "%");
        if (htmlPercent < 80) {
            output(ctx, "⚠️  WARNING: Low HTML content - many non-HTML pages");
        } else {
            output(ctx, "✅ GOOD: Mostly HTML content");
        }
        output(ctx, "");
    }
    
    private static void analyzePageSizes(List<Row> sample, FlameContext ctx) {
        output(ctx, "4. PAGE SIZE DISTRIBUTION");
        output(ctx, "-".repeat(80));
        
        List<Integer> sizes = new ArrayList<>();
        for (Row row : sample) {
            String length = row.get("length");
            if (length != null) {
                try {
                    sizes.add(Integer.parseInt(length));
                } catch (NumberFormatException e) {
                    // Skip invalid lengths
                }
            }
        }
        
        if (sizes.isEmpty()) {
            output(ctx,"No size information available");
            output(ctx, "");
            return;
        }
        
        Collections.sort(sizes);
        int count = sizes.size();
        
        output(ctx,"Total pages with size info: " + count);
        output(ctx,"Min size: " + sizes.get(0) + " bytes");
        output(ctx,"Max size: " + sizes.get(count - 1) + " bytes");
        output(ctx,"Median size: " + sizes.get(count / 2) + " bytes");
        output(ctx,"Average size: " + String.format("%.0f", sizes.stream().mapToInt(i -> i).average().orElse(0)) + " bytes");
        output(ctx, "");
        
        // Size distribution
        int tiny = 0, small = 0, medium = 0, large = 0, huge = 0;
        for (int size : sizes) {
            if (size < 1000) tiny++;
            else if (size < 10000) small++;
            else if (size < 100000) medium++;
            else if (size < 500000) large++;
            else huge++;
        }
        
        output(ctx, "Size distribution:");
        output(ctx, String.format("  < 1KB:     %6d (%5.2f%%)", tiny, (tiny * 100.0) / count));
        output(ctx, String.format("  1-10KB:    %6d (%5.2f%%)", small, (small * 100.0) / count));
        output(ctx, String.format("  10-100KB:  %6d (%5.2f%%)", medium, (medium * 100.0) / count));
        output(ctx, String.format("  100-500KB: %6d (%5.2f%%)", large, (large * 100.0) / count));
        output(ctx, String.format("  > 500KB:   %6d (%5.2f%%)", huge, (huge * 100.0) / count));
        output(ctx, "");
    }
    
    private static void analyzeLanguage(List<Row> sample, FlameContext ctx) {
        output(ctx, "5. LANGUAGE CHECK (English Content)");
        output(ctx, "-".repeat(80));
        
        int withPage = 0;
        int english = 0;
        int nonEnglish = 0;
        int noLangTag = 0;
        
        for (Row row : sample) {
            byte[] pageBytes = row.getBytes("page");
            if (pageBytes != null && pageBytes.length > 0) {
                withPage++;
                String html = new String(pageBytes, java.nio.charset.StandardCharsets.UTF_8);
                if (html.toLowerCase().contains("<html lang=\"en\">") || 
                    html.toLowerCase().contains("<html lang='en'>")) {
                    english++;
                } else if (html.toLowerCase().contains("<html lang=")) {
                    nonEnglish++;
                } else {
                    noLangTag++;
                }
            }
        }
        
        output(ctx, "Pages with content: " + withPage);
        output(ctx, "English (lang=\"en\"): " + english + " (" + 
            String.format("%.2f", (english * 100.0) / Math.max(1, withPage)) + "%)");
        output(ctx, "Non-English: " + nonEnglish + " (" + 
            String.format("%.2f", (nonEnglish * 100.0) / Math.max(1, withPage)) + "%)");
        output(ctx, "No lang tag: " + noLangTag + " (" + 
            String.format("%.2f", (noLangTag * 100.0) / Math.max(1, withPage)) + "%)");
        
        double englishPercent = (english * 100.0) / Math.max(1, withPage);
        if (englishPercent < 80) {
            output(ctx, "⚠️  WARNING: Low English content percentage");
        } else if (englishPercent < 95) {
            output(ctx, "⚠️  CAUTION: Some non-English content present");
        } else {
            output(ctx, "✅ GOOD: High English content percentage");
        }
        output(ctx, "");
    }
    
    private static void analyzeDomainDiversity(List<Row> sample, FlameContext ctx) {
        output(ctx, "6. DOMAIN DIVERSITY");
        output(ctx, "-".repeat(80));
        
        Set<String> domains = new HashSet<>();
        Set<String> tlds = new HashSet<>();
        
        for (Row row : sample) {
            String url = row.get("url");
            if (url != null) {
                try {
                    String[] parsed = URLParser.parseURL(url);
                    String host = parsed[1];
                    if (host != null) {
                        host = host.toLowerCase();
                        domains.add(host);
                        
                        // Extract TLD
                        String[] parts = host.split("\\.");
                        if (parts.length > 0) {
                            tlds.add(parts[parts.length - 1]);
                        }
                    }
                } catch (Exception e) {
                    // Skip invalid URLs
                }
            }
        }
        
        output(ctx,"Unique domains: " + domains.size());
        output(ctx,"Unique TLDs: " + tlds.size());
        output(ctx, "");
        
        if (domains.size() < 10) {
            output(ctx,"⚠️  WARNING: Very low domain diversity - only " + domains.size() + " unique domains");
        } else if (domains.size() < 50) {
            output(ctx,"⚠️  CAUTION: Low domain diversity - " + domains.size() + " unique domains");
        } else if (domains.size() < 200) {
            output(ctx,"✅ GOOD: Moderate domain diversity - " + domains.size() + " unique domains");
        } else {
            output(ctx,"✅ EXCELLENT: High domain diversity - " + domains.size() + " unique domains");
        }
        output(ctx, "");
    }
    
    private static void analyzeURLPatterns(List<Row> sample, FlameContext ctx) {
        output(ctx, "7. URL PATTERN ANALYSIS");
        output(ctx, "-".repeat(80));
        
        int deepPaths = 0;  // URLs with many path segments
        int queryParams = 0;  // URLs with query parameters
        int fragments = 0;  // URLs with fragments
        int longUrls = 0;  // Very long URLs
        
        for (Row row : sample) {
            String url = row.get("url");
            if (url != null) {
                try {
                    String[] parsed = URLParser.parseURL(url);
                    String path = parsed[3]; // path
                    
                    if (path != null) {
                        // Count path segments
                        String[] segments = path.split("/");
                        if (segments.length > 5) {
                            deepPaths++;
                        }
                        
                        // Check for query params
                        if (path.contains("?") || url.contains("?")) {
                            queryParams++;
                        }
                        
                        // Check for fragments
                        if (path.contains("#") || url.contains("#")) {
                            fragments++;
                        }
                    }
                    
                    // Check URL length
                    if (url.length() > 200) {
                        longUrls++;
                    }
                } catch (Exception e) {
                    // Skip invalid URLs
                }
            }
        }
        
        int total = sample.size();
        output(ctx, "Deep paths (>5 segments): " + deepPaths + " (" + 
            String.format("%.2f", (deepPaths * 100.0) / total) + "%)");
        output(ctx, "With query parameters: " + queryParams + " (" + 
            String.format("%.2f", (queryParams * 100.0) / total) + "%)");
        output(ctx, "With fragments (#): " + fragments + " (" + 
            String.format("%.2f", (fragments * 100.0) / total) + "%)");
        output(ctx, "Very long URLs (>200 chars): " + longUrls + " (" + 
            String.format("%.2f", (longUrls * 100.0) / total) + "%)");
        output(ctx, "");
    }
    
    private static void analyzeContentQuality(List<Row> sample, FlameContext ctx) {
        output(ctx, "8. CONTENT QUALITY INDICATORS");
        output(ctx, "-".repeat(80));
        
        int withTitle = 0;
        int withContent = 0;
        int emptyContent = 0;
        int veryShortContent = 0;  // < 100 chars
        int reasonableContent = 0;  // 100-50000 chars
        
        for (Row row : sample) {
            String title = row.get("title");
            byte[] pageBytes = row.getBytes("page");
            
            if (title != null && !title.isEmpty()) {
                withTitle++;
            }
            
            if (pageBytes != null && pageBytes.length > 0) {
                withContent++;
                int contentLength = pageBytes.length;
                if (contentLength < 100) {
                    veryShortContent++;
                } else if (contentLength <= 50000) {
                    reasonableContent++;
                }
            } else {
                emptyContent++;
            }
        }
        
        int total = sample.size();
        output(ctx,"Pages with title: " + withTitle + " (" + 
            String.format("%.2f", (withTitle * 100.0) / total) + "%)");
        output(ctx,"Pages with content: " + withContent + " (" + 
            String.format("%.2f", (withContent * 100.0) / total) + "%)");
        output(ctx,"Empty content: " + emptyContent + " (" + 
            String.format("%.2f", (emptyContent * 100.0) / total) + "%)");
        output(ctx,"Very short content (<100 chars): " + veryShortContent + " (" + 
            String.format("%.2f", (veryShortContent * 100.0) / total) + "%)");
        output(ctx,"Reasonable content (100-50KB): " + reasonableContent + " (" + 
            String.format("%.2f", (reasonableContent * 100.0) / total) + "%)");
        
        double qualityScore = ((withTitle * 0.3) + (withContent * 0.5) + (reasonableContent * 0.2)) / total * 100;
        output(ctx, "");
        output(ctx,"Overall quality score: " + String.format("%.2f", qualityScore) + "%");
        if (qualityScore < 60) {
            output(ctx,"⚠️  WARNING: Low quality score - many pages missing titles or content");
        } else if (qualityScore < 80) {
            output(ctx,"⚠️  CAUTION: Moderate quality - room for improvement");
        } else {
            output(ctx,"✅ GOOD: High quality content");
        }
        output(ctx, "");
    }
    
    private static String truncate(String s, int maxLen) {
        if (s == null) return "";
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen - 3) + "...";
    }
}

