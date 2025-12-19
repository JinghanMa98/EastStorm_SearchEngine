package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {

  private static final String UA = "cis5550-crawler";
  private static final String PT_CRAWL = "pt-crawl";
  private static final String HOSTS_TB = "hosts";
  private static final String ROBOTS_TB = "robots";
  // Optimized for speed: reduced MAX_BYTES (HTML pages are usually < 500KB)
  // and reduced timeout for faster failure detection
  private static final int MAX_BYTES = 500_000;  // 500KB (reduced from 1MB)
  private static final int TIMEOUT_MS = 5_000;    // 5 seconds (reduced from 10s)
  private static final int MIN_PAGES = 100;
  private static final int ITERATION_DELAY_MS = 25;  // Reduced from 50ms for faster iterations

  public static void run(FlameContext ctx, String[] args) throws Exception {
    if (args == null || args.length < 1 || args[0] == null || args[0].isEmpty()) {
      ctx.output("ERROR: need at least one seed URL");
      return;
    }

    // Support multiple seed URLs
    List<String> seeds = new ArrayList<>();
    for (String arg : args) {
      if (arg != null && !arg.isEmpty()) {
        try {
          seeds.add(normalizeSeed(arg));
        } catch (Exception e) {
          ctx.output("WARNING: Skipping invalid seed URL: " + arg);
        }
      }
    }
    
    if (seeds.isEmpty()) {
      ctx.output("ERROR: No valid seed URLs provided");
      return;
    }
    
    FlameRDD frontier = ctx.parallelize(seeds);
    ctx.output("Starting crawler with " + seeds.size() + " seed URL(s)");

    int iterations = 0;
    while (true) {
      long crawledCount = ctx.getKVS().count(PT_CRAWL);
      long frontierCount = frontier.count();
      iterations++;
      
      // Log progress every iteration for debugging
      if (iterations % 3 == 0 || iterations == 1) {
        ctx.output("Iteration " + iterations + ": " + crawledCount + " pages crawled, " + frontierCount + " URLs in frontier");
      }
      
      // If frontier is empty but we haven't reached minimum, try to find more URLs
      if (frontierCount == 0 && crawledCount < MIN_PAGES) {
        ctx.output("Frontier empty with " + crawledCount + " pages. Searching for more URLs...");
        
        // Try multiple times to find new URLs
        for (int retry = 0; retry < 5; retry++) {
          Thread.sleep(3000); // Wait longer for politeness delays to expire
          
          // Scan crawled pages to find URLs that haven't been crawled yet
          if (crawledCount > 0 && crawledCount < MIN_PAGES) {
            // Get all URLs from crawled pages, filter out already-crawled ones
            FlameRDD allUrlsFromPages = ctx.fromTable(PT_CRAWL, row -> {
              String pageUrl = row.get("url");
              String page = row.get("page");
              if (pageUrl == null || page == null) return null;
              
              // Extract links from this page
              List<String> hrefs = extractLinks(page);
              List<String> validUrls = new ArrayList<>();
              
              for (String href : hrefs) {
                try {
                  String abs = normalizeAgainst(pageUrl, href);
                  if (abs == null) continue;
                  //////////
                  // ===== Vivian Blacklist START =====
                  try {
                      String host = new URL(abs).getHost().toLowerCase();
                      String lower = abs.toLowerCase();

                      // ARCHIVE INFINITE LOOPS
                      if (host.contains("archive.org") ||
                          host.contains("web.archive.org") ||
                          host.contains("wayback"))
                          continue;

                      // WIKI MIRRORS / CONTENT FARMS
                      if (host.contains("wikiwand.") ||
                          host.contains("wikihow.") ||
                          host.contains("miraheze.") ||
                          host.contains("fandom.com") ||
                          host.contains("wikidot.") ||
                          host.contains("wiki.gg") ||
                          host.contains("wikipedia-on-ipfs"))
                          continue;

                      // GLOBAL GIANTS
                      if (host.contains("google.") ||
                          host.contains("gstatic.") ||
                          host.contains("facebook.") ||
                          host.contains("instagram.") ||
                          host.contains("youtube.") ||
                          host.contains("twitter.") ||
                          host.contains("tiktok.") ||
                          host.contains("amazon.") ||
                          host.contains("bing.") ||
                          host.contains("yahoo.") ||
                          host.contains("doubleclick."))
                          continue;

                      // ASIA PORTALS
                      if (host.contains("baidu.") ||
                          host.contains("qq.") ||
                          host.contains("naver.") ||
                          host.contains("kakao."))
                          continue;

                      // ONLY allow English Wikipedia family
                      if (host.endsWith(".wikipedia.org") ||
                          host.endsWith(".wiktionary.org") ||
                          host.endsWith(".wikiquote.org") ||
                          host.endsWith(".wikibooks.org") ||
                          host.endsWith(".wikinews.org") ||
                          host.endsWith(".wikisource.org") ||
                          host.endsWith(".wikivoyage.org") ||
                          host.endsWith(".wikiversity.org") ||
                          host.endsWith(".wikidata.org")) {
                          if (!host.startsWith("en.")) continue;
                      }

                      // ACADEMIC PDFs / GIANTS
                      if (host.contains("jstor.org") ||
                          host.contains("dl.acm.org") ||
                          host.contains("doi.org") ||
                          host.contains("plato.stanford.edu") ||
                          host.contains("oxfordhandbooks.") ||
                          host.contains("worldbank.org") ||
                          host.contains("semanticscholar") ||
                          host.contains("core.ac.uk"))
                          continue;

                      // NEWS / MAGAZINES
                      if (host.contains("nytimes.com") ||
                          host.contains("newyorker.com") ||
                          host.contains("slate.com") ||
                          host.contains("vox.com") ||
                          host.contains("usatoday.") ||
                          host.contains("adage.com") ||
                          host.contains("tempo.co") ||
                          host.contains("tribunnews.com") ||
                          host.contains("cambridge.org"))
                          continue;

                      // CONTENT FARMS / BLOG PLATFORMS
                      if (host.contains("medium.com") ||
                          host.contains("wordpress.com") ||
                          host.contains("blogspot.") ||
                          host.contains("tumblr.com") ||
                          host.contains("hubpages.com") ||
                          host.contains("ezinearticles.") ||
                          host.contains("cloudfront.net") ||
                          host.contains("b-cdn.net"))
                          continue;

                      // FORUMS
                      if (host.contains("reddit.com") ||
                          host.contains("stackoverflow.") ||
                          host.contains("stackexchange.") ||
                          host.contains("discord.com") ||
                          host.contains("4chan.org"))
                          continue;

                      // COMMERCIAL LISTINGS
                      if (host.contains("tripadvisor.") ||
                          host.contains("booking.") ||
                          host.contains("airbnb.") ||
                          host.contains("zillow.") ||
                          host.contains("realtor.") ||
                          host.contains("autotrader."))
                          continue;

                      // COUNTRY NON-ENGLISH DOMAINS
                      if (host.endsWith(".ru") ||
                          host.endsWith(".cn") ||
                          host.endsWith(".jp") ||
                          host.endsWith(".kr") ||
                          host.endsWith(".tr") ||
                          host.endsWith(".vn") ||
                          host.endsWith(".id") ||
                          host.endsWith(".ir") ||
                          host.endsWith(".th"))
                          continue;

                      // TRACKING / AD / REDIRECTS
                      if (host.contains("bit.ly") ||
                          host.contains("tinyurl") ||
                          host.contains("t.co") ||
                          host.contains("lnkd.in") ||
                          host.contains("feedburner.") ||
                          host.contains("mailchi.mp") ||
                          host.contains("clickserve.") ||
                          host.contains("adserver.") ||
                          host.contains("doubleverify.") ||
                          host.contains("googletagmanager.") ||
                          host.contains("googletagservices."))
                          continue;

                      // FILES / DOWNLOADS
                      if (lower.endsWith(".pdf") || lower.endsWith(".zip") ||
                          lower.endsWith(".tar") || lower.endsWith(".gz") ||
                          lower.endsWith(".rar") || lower.endsWith(".7z") ||
                          lower.endsWith(".epub") ||
                          lower.endsWith(".png") || lower.endsWith(".jpg") ||
                          lower.endsWith(".jpeg") || lower.endsWith(".gif") ||
                          lower.endsWith(".svg") ||
                          lower.contains("download"))
                          continue;

                      // PAGINATION TRAPS
                      if (abs.contains("?page=") ||
                          abs.contains("&page=") ||
                          abs.contains("?search=") ||
                          abs.contains("?query=") ||
                          abs.contains("&offset=") ||
                          abs.contains("&start="))
                          continue;

                  } catch (Exception e) {
                      // If blacklist check fails, safely skip URL
                      continue;
                  }
                  // ===== Vivian Blacklist END =====


                  ////////
                  if (!isCrawlable(abs)) continue;
                  if (filteredByExt(abs)) continue;
                  validUrls.add(abs);
                } catch (Exception e) {
                  continue;
                }
              }
              
              return validUrls.isEmpty() ? null : String.join("\n", validUrls);
            });
            
            // Flatten and get distinct URLs
            FlameRDD candidateUrls = allUrlsFromPages.flatMap(line -> {
              if (line == null) return Collections.emptyList();
              List<String> urls = new ArrayList<>();
              for (String u : line.split("\n")) {
                if (u != null && !u.isEmpty()) urls.add(u);
              }
              return urls;
            }).distinct();
            
            // Filter out URLs that have already been successfully crawled (have "page" column)
            // Include URLs that were attempted but failed (no "page" column) - allow retrying when stuck
            KVSClient kvs = ctx.getKVS();
            FlameRDD newUrls = candidateUrls.filter(candidateUrl -> {
              try {
                String rowKey = Hasher.hash(candidateUrl);
                // Use existsRow for faster check, then getRow only if needed
                if (!kvs.existsRow(PT_CRAWL, rowKey)) {
                  return true; // Not attempted yet
                }
                // Check if it was successfully crawled (has "page" column)
                Row row = kvs.getRow(PT_CRAWL, rowKey);
                if (row != null && row.get("page") != null) {
                  return false; // Already successfully crawled
                }
                // Has row but no "page" - was attempted but failed
                // Allow retrying when we're stuck and haven't reached minimum pages
                // This helps us discover more URLs when the frontier is empty
                return true; // Allow retry of failed URLs
              } catch (Exception e) {
                return true; // On error, include it (might be new)
              }
            });
            
            long newCount = newUrls.count();
            if (newCount > 0) {
              ctx.output("Found " + newCount + " new URLs to crawl (retry " + (retry + 1) + ")");
              frontier = newUrls;
              break; // Found new URLs, continue crawling
            } else if (retry < 2) {
              ctx.output("No new URLs found, retrying... (retry " + (retry + 1) + "/3)");
            }
          }
        }
        
        // Check if we found new URLs
        frontierCount = frontier.count();
        if (frontierCount == 0) {
          ctx.output("Frontier still empty with only " + crawledCount + " pages. Need " + MIN_PAGES + " pages.");
          ctx.output("Try using multiple seed URLs or a seed URL with more links.");
          break;
        }
      }
      
      // Process frontier
      frontier = frontier.flatMap((FlameRDD.StringToIterable) (String url) -> {
        ArrayList<String> out = new ArrayList<>();
        
        // Filter non-English Wikipedia URLs early (before any network requests)
        if (isNonEnglishWikipedia(url)) {
          return out; // Skip non-English Wikipedia pages
        }
        
        KVSClient kvs = ctx.getKVS();

        final String rowKey = Hasher.hash(url);
        // Only skip if it was successfully crawled (has "page" column)
        // Allow retrying URLs that were attempted but failed
        // Row existingRow = kvs.getRow(PT_CRAWL, rowKey); Vivian modified
        
        //////////Vivian added/////////////////
        Row existingRow = null;
        try {
            existingRow = kvs.getRow(PT_CRAWL, rowKey);
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Decoding error")) {
                return out; 
            }
            throw e; 
        }
        ///////////////// Vivian added finish/////////
        
        if (existingRow != null && existingRow.get("page") != null) {
          // Already successfully crawled - skip silently (too many to log)
          return out;
        }

        Host h = Host.from(url);
        Robots rules = null;
        try {
          rules = Robots.ensure(kvs, h);
        } catch (Exception e) {
          // If robots.txt fetch fails (e.g., UnknownHostException), use default rules
          rules = new Robots(Collections.emptyList(), -1);
        }
        if (rules != null && !rules.allows(h.pathWithQuery)) {
          return out; // robots says no: never attempt HEAD → no row written
        }

        final long delayMs = (rules != null && rules.delayMs >= 0) ? rules.delayMs : 1000;
        long now = System.currentTimeMillis();
        long last = kvsGetLong(kvs, HOSTS_TB, h.hostKey(), "last", 0);
        if (last > 0 && (now - last) < delayMs) {
          out.add(url); // try again next round
          return out;
        }

        // HEAD request
        HttpURLConnection headConn = null;
        try {
          headConn = open(url, "HEAD");
        } catch (Exception e) {
          // If connection fails (e.g., UnknownHostException), skip this URL
          return out;
        }
        int headCode = safeCode(headConn);
        String headCT = baseMime(headConn.getContentType());
        String headLen = headConn.getHeaderField("Content-Length");

        // Use putRow() for persistent tables to store all columns together
        Row pageRow = new Row(rowKey);
        pageRow.put("url", url);
        pageRow.put("responseCode", Integer.toString(headCode));
        if (headCT != null) pageRow.put("contentType", headCT);
        if (headLen != null) pageRow.put("length", headLen);
        kvs.putRow(PT_CRAWL, pageRow);

        kvs.put(HOSTS_TB, h.hostKey(), "last", Long.toString(now));

        // Redirects → enqueue Location and stop
        if (isRedirect(headCode)) {
          String loc = headConn.getHeaderField("Location");
          if (loc != null && !loc.isEmpty()) {
            String next = normalizeAgainst(url, loc);
            if (next != null && isCrawlable(next) && !filteredByExt(next)) out.add(next);
          }
          return out;
        }

        // Only GET if 200 + text/html
        if (headCode != 200 || !"text/html".equals(headCT)) return out;

        HttpURLConnection getConn = null;
        try {
          getConn = open(url, "GET");
        } catch (Exception e) {
          // If connection fails (e.g., UnknownHostException), skip this URL
          return out;
        }
        int getCode = safeCode(getConn);

        if (getCode != 200) {
          pageRow.put("responseCode", Integer.toString(getCode));
          kvs.putRow(PT_CRAWL, pageRow);
          return out;
        }

        byte[] body = readCapped(getConn.getInputStream(), MAX_BYTES);
        
        // Extract and store title
        String html = new String(body, StandardCharsets.UTF_8);
        
        // Only crawl English content: check for <html lang="en">
        if (!isEnglishContent(html)) {
          // Not English - skip storing page content and extracting links
          // But still store the HEAD request info (already in pageRow)
          kvs.putRow(PT_CRAWL, pageRow);
          return out;
        }
        
        pageRow.put("page", body); // store exact bytes
        String title = extractTitle(html);
        if (title != null && !title.isEmpty()) {
          pageRow.put("title", title);
        }
        kvs.putRow(PT_CRAWL, pageRow);

        List<String> hrefs = extractLinks(html);

        LinkedHashSet<String> uniq = new LinkedHashSet<>();
        for (String href : hrefs) {
          String abs = normalizeAgainst(url, href);
          if (abs == null) continue;
          if (!isCrawlable(abs)) continue;
          if (filteredByExt(abs)) continue;
          // Filter non-English Wikipedia URLs early (before downloading)
          if (isNonEnglishWikipedia(abs)) continue;
          // Don't check if already crawled here - let the main loop handle it
          // This allows the frontier to have more URLs, and the main loop will skip already-crawled ones
          uniq.add(abs);
        }
        out.addAll(uniq);
        return out;
      }).distinct();
      
      // Check frontier after processing
      long newFrontierCount = frontier.count();
      long newCrawledCount = ctx.getKVS().count(PT_CRAWL);
      
      if (iterations == 1 || newFrontierCount == 0 || iterations % 5 == 0) {
        ctx.output("After iteration " + iterations + ": " + newCrawledCount + " pages crawled, " + newFrontierCount + " URLs in frontier");
      }
      
      // Stop if we've reached minimum pages and frontier is empty
      if (newCrawledCount >= MIN_PAGES && newFrontierCount == 0) {
        ctx.output("Stopping: " + newCrawledCount + " pages crawled (>= " + MIN_PAGES + "), frontier is empty");
        break;
      }
      
      Thread.sleep(ITERATION_DELAY_MS); // tiny breath; true politeness is per-host above
    }

    long finalCount = ctx.getKVS().count(PT_CRAWL);
    if (finalCount < MIN_PAGES) {
      ctx.output("WARNING: Crawled only " + finalCount + " pages, but minimum is " + MIN_PAGES + ". Consider using multiple seed URLs or a seed with more links.");
    } else {
      ctx.output("OK: Crawled " + finalCount + " pages");
    }
  }

  /* -------------------- Helpers -------------------- */

  private static class Host {
    final String scheme, host, pathWithQuery;
    final int port;

    Host(String s, String h, int p, String pq) { scheme = s; host = h; port = p; pathWithQuery = pq; }

    static Host from(String absUrl) throws Exception {
      String[] p = URLParser.parseURL(absUrl);
      String scheme = (p[0] == null) ? "http" : p[0].toLowerCase();
      String host = (p[1] == null) ? "" : p[1].toLowerCase();
      int port = -1;
      if (p[2] != null && !p[2].isEmpty()) try { port = Integer.parseInt(p[2]); } catch (Exception ignored) {}
      if (port == -1) port = scheme.equals("https") ? 443 : 80;
      String pq = (p[3] == null || p[3].isEmpty()) ? "/" : (p[3].startsWith("/") ? p[3] : "/" + p[3]);
      int hash = pq.indexOf('#'); if (hash >= 0) pq = pq.substring(0, hash);
      return new Host(scheme, host, port, pq);
    }

    String origin()  { return scheme + "://" + host + ":" + port; }
    String hostKey() { return origin(); }
  }

  private static class Robots {
    final List<Rule> rules;
    final long delayMs; // -1 if none

    Robots(List<Rule> r, long d) { rules = r; delayMs = d; }

    static class Rule { final boolean allow; final String prefix; Rule(boolean a, String p){allow=a;prefix=p;} }

    boolean allows(String path) {
      String p = (path == null || path.isEmpty()) ? "/" : path;
      for (Rule r : rules) {
        if (r.prefix.isEmpty()) return r.allow;      // empty Disallow => allow all
        if (p.startsWith(r.prefix)) return r.allow;  // first match wins
      }
      return true;
    }

    static Robots ensure(KVSClient kvs, Host h) throws Exception {
      String state = kvsGetString(kvs, ROBOTS_TB, h.hostKey(), "state");
      if ("missing".equals(state)) return new Robots(Collections.emptyList(), -1);
      if ("fetched".equals(state)) {
        String raw = kvsGetString(kvs, ROBOTS_TB, h.hostKey(), "body");
        return parse(raw);
      }

      String robotsUrl = h.origin() + "/robots.txt";
      HttpURLConnection c = null;
      try {
        c = open(robotsUrl, "GET"); // no delay for robots
      } catch (Exception e) {
        // If robots.txt fetch fails (e.g., UnknownHostException), mark as missing
        kvs.put(ROBOTS_TB, h.hostKey(), "state", "missing");
        return new Robots(Collections.emptyList(), -1);
      }
      int code = safeCode(c);
      if (code != 200) {
        kvs.put(ROBOTS_TB, h.hostKey(), "state", "missing");
        return new Robots(Collections.emptyList(), -1);
      }
      byte[] body = readCapped(c.getInputStream(), MAX_BYTES);
      String txt = new String(body, StandardCharsets.UTF_8);
      kvs.put(ROBOTS_TB, h.hostKey(), "state", "fetched");
      kvs.put(ROBOTS_TB, h.hostKey(), "body", txt);
      return parse(txt);
    }

    static Robots parse(String txt) {
      if (txt == null) return new Robots(Collections.emptyList(), -1);

      Map<String, List<String>> byUA = new LinkedHashMap<>();
      String curUA = null;
      for (String rawLine : txt.split("\n")) {
        String line = rawLine.split("#", 2)[0].trim();
        if (line.isEmpty()) continue;
        String low = line.toLowerCase(Locale.ROOT);
        if (low.startsWith("user-agent:")) {
          curUA = line.substring(11).trim().toLowerCase(Locale.ROOT);
          byUA.computeIfAbsent(curUA, k -> new ArrayList<>());
        } else if (curUA != null) {
          byUA.get(curUA).add(line);
        }
      }

      List<String> chosen = byUA.containsKey(UA) ? byUA.get(UA)
                            : byUA.getOrDefault("*", Collections.emptyList());

      long delay = -1;
      List<Rule> rules = new ArrayList<>();
      for (String line : chosen) {
        String low = line.toLowerCase(Locale.ROOT);
        if (low.startsWith("allow:")) {
          String pre = line.substring(6).trim();
          rules.add(new Rule(true, pre));
        } else if (low.startsWith("disallow:")) {
          String pre = line.substring(9).trim();
          if (pre.isEmpty()) rules.add(new Rule(true, "")); // empty Disallow => allow all
          else rules.add(new Rule(false, pre));
        } else if (low.startsWith("crawl-delay:")) {
          try {
            double s = Double.parseDouble(line.substring(12).trim());
            delay = Math.max(delay, Math.round(s * 1000.0));
          } catch (Exception ignored) {}
        }
      }
      return new Robots(rules, delay);
    }
  }

  private static HttpURLConnection open(String url, String method) throws Exception {
    URL u = new URI(url).toURL();
    HttpURLConnection c = (HttpURLConnection) u.openConnection();
    c.setInstanceFollowRedirects(false);
    c.setRequestMethod(method);
    c.setRequestProperty("User-Agent", UA);
    if ("GET".equals(method)) {
      c.setRequestProperty("Accept", "text/html, text/plain;q=0.9, */*;q=0.8");
    }
    c.setConnectTimeout(TIMEOUT_MS);
    c.setReadTimeout(TIMEOUT_MS);
    c.connect();
    return c;
  }

  private static int safeCode(HttpURLConnection c) {
    try { return c.getResponseCode(); } catch (Exception e) { return 599; }
  }

  private static byte[] readCapped(InputStream in, int cap) throws Exception {
    if (in == null) return new byte[0];
    ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.min(cap, 32768));
    byte[] buf = new byte[8192];
    int n, total = 0;
    while ((n = in.read(buf)) > 0) {
      if (total + n > cap) { baos.write(buf, 0, cap - total); break; }
      baos.write(buf, 0, n); total += n;
    }
    return baos.toByteArray();
  }

  private static boolean isRedirect(int code) {
    return code == 301 || code == 302 || code == 303 || code == 307 || code == 308;
  }

  private static String baseMime(String ct) {
    if (ct == null) return null;
    String[] parts = ct.split(";", 2);
    return parts[0].trim().toLowerCase(Locale.ROOT);
  }

  private static boolean isCrawlable(String u) throws Exception {
    String[] p = URLParser.parseURL(u);
    String scheme = (p[0] == null) ? "" : p[0].toLowerCase();
    return "http".equals(scheme) || "https".equals(scheme);
  }

  private static boolean filteredByExt(String u) {
    String uu = u.toLowerCase(Locale.ROOT);
    return uu.endsWith(".jpg") || uu.endsWith(".jpeg") || uu.endsWith(".gif")
        || uu.endsWith(".png") || uu.endsWith(".txt");
  }

  private static String normalizeSeed(String url) throws Exception {
    String[] p = URLParser.parseURL(url);
    String scheme = (p[0] == null || p[0].isEmpty()) ? "http" : p[0].toLowerCase();
    String host   = (p[1] == null) ? "" : p[1].toLowerCase();
    int port = -1;
    if (p[2] != null && !p[2].isEmpty()) try { port = Integer.parseInt(p[2]); } catch (Exception ignored) {}
    if (port == -1) port = scheme.equals("https") ? 443 : 80;
    String pathQ = (p[3] == null || p[3].isEmpty()) ? "/" : (p[3].startsWith("/") ? p[3] : "/" + p[3]);
    int hash = pathQ.indexOf('#'); if (hash >= 0) pathQ = pathQ.substring(0, hash);
    return scheme + "://" + host + ":" + port + pathQ;
  }

  public static String normalizeURL(String base, String href) throws Exception {
    return normalizeAgainst(base, href);
  }

  private static String normalizeAgainst(String base, String href) throws Exception {
    if (href == null || href.isEmpty()) return null;
    if (href.startsWith("#")) return normalizeSeed(base);
    
    // Skip non-HTTP protocols (javascript:, mailto:, tel:, irc:, etc.)
    // Check before any URI operations to avoid exceptions
    String hrefLower = href.toLowerCase().trim();
    
    // FIRST: Check if href contains any non-HTTP protocol strings
    if (hrefLower.contains("irc:") || hrefLower.contains("javascript:") || 
        hrefLower.contains("mailto:") || hrefLower.contains("tel:") ||
        hrefLower.contains("ftp:") || hrefLower.contains("file:") ||
        hrefLower.contains("news:") || hrefLower.contains("nntp:")) {
      return null; // Contains non-HTTP protocol, skip immediately
    }
    
    // Check for absolute URLs with non-HTTP protocols
    if (hrefLower.contains(":")) {
      String protocol = hrefLower.split(":")[0].trim();
      // Explicitly check for common non-HTTP protocols - be very aggressive
      if (!protocol.isEmpty() && !protocol.equals("http") && !protocol.equals("https")) {
        // Common non-HTTP protocols to skip - comprehensive list
        if (protocol.equals("javascript") || protocol.equals("mailto") || protocol.equals("tel") ||
            protocol.equals("irc") || protocol.equals("ftp") || protocol.equals("file") ||
            protocol.equals("data") || protocol.equals("about") || protocol.equals("chrome") ||
            protocol.equals("moz") || protocol.equals("news") || protocol.equals("nntp") ||
            protocol.equals("gopher") || protocol.equals("wais") || protocol.startsWith("x-")) {
          return null;
        }
        // For ANY other non-http/https protocol, skip it to avoid exceptions
        // This is safer than trying to resolve unknown protocols
        return null;
      }
    }
    
    if (href.startsWith("//")) {
      String[] p = URLParser.parseURL(base);
      String scheme = (p[0] == null || p[0].isEmpty()) ? "http" : p[0].toLowerCase();
      href = scheme + ":" + href;
    }
    
    try {
      // Check the href string one more time before URI resolution
      String hrefCheck = href.toLowerCase();
      if (hrefCheck.startsWith("irc:") || hrefCheck.startsWith("javascript:") ||
          hrefCheck.startsWith("mailto:") || hrefCheck.startsWith("tel:") ||
          hrefCheck.startsWith("ftp:") || hrefCheck.startsWith("file:")) {
        return null;
      }
      
      URI resolved;
      try {
        resolved = new URI(base).resolve(href);
      } catch (Exception e) {
        // If URI resolution fails (e.g., due to irc: protocol), skip it
        return null;
      }
      
      // CRITICAL: Check scheme from URI object FIRST, before calling toString() or anything else
      // This prevents toURL() from being called on non-HTTP URIs
      String uriScheme = resolved.getScheme();
      if (uriScheme == null) {
        // No scheme - might be a relative URI, but skip it to be safe
        return null;
      }
      String schemeLower = uriScheme.toLowerCase();
      // Reject any non-HTTP schemes IMMEDIATELY to avoid toURL() exceptions
      if (!schemeLower.equals("http") && !schemeLower.equals("https")) {
        return null; // Non-HTTP scheme, skip it BEFORE any other operations
      }
      
      // Now safe to call toString() since we know it's http/https
      String resolvedStr = resolved.toString();
      
      // Double-check protocol after resolution - catch any that slipped through
      String resolvedLower = resolvedStr.toLowerCase();
      if (resolvedLower.startsWith("irc:") || resolvedLower.startsWith("javascript:") ||
          resolvedLower.startsWith("mailto:") || resolvedLower.startsWith("tel:") ||
          resolvedLower.startsWith("ftp:") || resolvedLower.startsWith("file:") ||
          resolvedLower.startsWith("news:") || resolvedLower.startsWith("nntp:")) {
        return null;
      }
      
      // Only parse if we have http/https scheme to avoid toURL() errors
      String[] q;
      try {
        q = URLParser.parseURL(resolvedStr);
      } catch (Exception e) {
        // If parsing fails (e.g., due to protocol issues), skip it
        return null;
      }
      String scheme = (q[0] == null) ? "http" : q[0].toLowerCase();
      
      // Final check - only allow http and https
      if (!scheme.equals("http") && !scheme.equals("https")) {
        return null;
      }
      
      String host   = (q[1] == null) ? "" : q[1].toLowerCase();
      int port = -1;
      if (q[2] != null && !q[2].isEmpty()) try { port = Integer.parseInt(q[2]); } catch (Exception ignored) {}
      if (port == -1) port = scheme.equals("https") ? 443 : 80;
      String pathQ = (q[3] == null || q[3].isEmpty()) ? "/" : (q[3].startsWith("/") ? q[3] : "/" + q[3]);
      int hash = pathQ.indexOf('#'); if (hash >= 0) pathQ = pathQ.substring(0, hash);
      return scheme + "://" + host + ":" + port + pathQ;
    } catch (Exception e) {
      // Handle MalformedURLException and other URI/URL errors (e.g., irc:, unknown protocols)
      // This is the final safety net
      return null;
    }
  }

  private static final Pattern A_TAG = Pattern.compile("(?is)<\\s*a\\b[^>]*>");
  private static final Pattern HREF_ATTR = Pattern.compile("(?is)href\\s*=\\s*(\"([^\"]*)\"|'([^']*)'|([^\\s>]+))");
  private static final Pattern TITLE_TAG = Pattern.compile("(?is)<\\s*title\\b[^>]*>(.*?)<\\s*/\\s*title\\s*>");
  private static final Pattern HTML_LANG_EN = Pattern.compile("(?is)<\\s*html[^>]*\\s+lang\\s*=\\s*[\"']en[\"']", Pattern.CASE_INSENSITIVE);

  /**
   * Check if a URL is a non-English Wikipedia page.
   * Wikipedia URLs follow the pattern: <lang>.wikipedia.org
   * We only want en.wikipedia.org (English).
   * This allows us to filter BEFORE downloading, saving bandwidth and time.
   */
  private static boolean isNonEnglishWikipedia(String url) {
    if (url == null) return false;
    try {
      String[] parsed = URLParser.parseURL(url);
      String host = parsed[1]; // host
      if (host == null) return false;
      host = host.toLowerCase();
      
      // Check if it's a Wikipedia domain
      if (host.contains("wikipedia.org")) {
        // Extract language code (e.g., "zh" from "zh.wikipedia.org")
        String[] parts = host.split("\\.");
        if (parts.length >= 2) {
          String langCode = parts[0];
          // Only allow "en" (English) Wikipedia
          // Also allow "www.wikipedia.org" (redirects to en.wikipedia.org)
          return !langCode.equals("en") && !langCode.equals("www");
        }
      }
      return false;
    } catch (Exception e) {
      // If URL parsing fails, don't filter (let other checks handle it)
      return false;
    }
  }

  static boolean isEnglishContent(String html) {
    if (html == null || html.isEmpty()) return false;
    // Check for <html lang="en"> or <html lang='en'> (case-insensitive)
    Matcher m = HTML_LANG_EN.matcher(html);
    return m.find();
  }

  static String extractTitle(String html) {
    if (html == null) return null;
    Matcher m = TITLE_TAG.matcher(html);
    if (m.find()) {
      String title = m.group(1);
      if (title != null) {
        // Clean up the title: remove HTML entities, trim whitespace
        title = title.replaceAll("&nbsp;", " ")
                     .replaceAll("&amp;", "&")
                     .replaceAll("&lt;", "<")
                     .replaceAll("&gt;", ">")
                     .replaceAll("&quot;", "\"")
                     .replaceAll("&#39;", "'")
                     .replaceAll("&[a-zA-Z0-9]+;", " ") // other entities
                     .trim();
        // Limit title length
        if (title.length() > 200) {
          title = title.substring(0, 197) + "...";
        }
        return title;
      }
    }
    return null;
  }

  static List<String> extractLinks(String html) {
    ArrayList<String> out = new ArrayList<>();
    if (html == null || html.isEmpty()) return out;
    Matcher m = A_TAG.matcher(html);
    while (m.find()) {
      String tag = m.group();
      Matcher h = HREF_ATTR.matcher(tag);
      if (h.find()) {
        String val = h.group(2); if (val == null) val = h.group(3); if (val == null) val = h.group(4);
        if (val != null && !val.isEmpty()) out.add(val.trim());
      }
    }
    return out;
  }

  private static String kvsGetString(KVSClient kvs, String table, String row, String col) throws Exception {
    byte[] b = kvs.get(table, row, col);
    return (b == null) ? null : new String(b, StandardCharsets.UTF_8);
  }
  private static long kvsGetLong(KVSClient kvs, String table, String row, String col, long dflt) throws Exception {
    String s = kvsGetString(kvs, table, row, col);
    try { return (s == null) ? dflt : Long.parseLong(s); } catch (Exception e) { return dflt; }
  }
}
