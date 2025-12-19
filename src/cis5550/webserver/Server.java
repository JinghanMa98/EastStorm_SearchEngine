package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.util.*;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

public class Server {
	private static volatile Server INSTANCE;
	  private static volatile boolean started = false;
	  private static volatile int configuredPort = 80;
	  private static volatile boolean useHttps = false;
	  private static volatile String keyStorePath = null;
	  private static volatile String keyStorePassword = null;

	  // ---------- API ----------
	  public static void port(int p) { configuredPort = p; }

	  public static void securePort(int p, String ksPath, String password) {
	    configuredPort = p;
	    keyStorePath = ksPath;
	    keyStorePassword = password;
	    useHttps = true;
	  }

	  public static class staticFiles {
	    public static void location(String p) {
	      ensureInstance();
	      INSTANCE.staticRoot = Paths.get(p).toAbsolutePath().normalize();
	      startIfNeeded();
	    }
	  }

	  public static void get (String path, Route r){ ensureInstance(); INSTANCE.addRoute("GET",  path, r); startIfNeeded(); }
	  public static void post(String path, Route r){ ensureInstance(); INSTANCE.addRoute("POST", path, r); startIfNeeded(); }
	  public static void put (String path, Route r){ ensureInstance(); INSTANCE.addRoute("PUT",  path, r); startIfNeeded(); }

	  private static void ensureInstance() {
		    if (INSTANCE == null) {
		      synchronized (Server.class) {
		        if (INSTANCE == null) INSTANCE = new Server();
		      }
		    }
		  }
	  private static void startIfNeeded() {
		    if (!started) {
		      synchronized (Server.class) {
		        if (!started) {
		          started = true;
		          Thread t = new Thread(() -> {
		            try { INSTANCE.run(); } catch (Throwable e) { e.printStackTrace(); }
		          }, "cis5550-server");
		          t.start();
		        }
		      }
		    }
		  }

  private static class RouteEntry {
    final String method, pattern; final Route handler;
    RouteEntry(String m, String p, Route h){ method=m; pattern=p; handler=h; }
  }
  // ---------- Instance ----------
  private final List<RouteEntry> routes = Collections.synchronizedList(new ArrayList<>());
  private Path staticRoot = null;

  private void addRoute(String m, String p, Route h){ routes.add(new RouteEntry(m,p,h)); }


  void run() {
	    try {
	      ServerSocket ss;
	      if (useHttps) {
	        System.out.println("Starting HTTPS on port " + configuredPort);

	        SSLServerSocketFactory factory = createSSLServerSocketFactory(keyStorePath, keyStorePassword);
	        ss = factory.createServerSocket(configuredPort);

	      } else {
	        System.out.println("Starting HTTP on port " + configuredPort);
	        ss = new ServerSocket(configuredPort);
	      }

	      System.out.println("CIS5550 listening on port " + configuredPort);

	      while (true) {
	        final Socket sock = ss.accept();
	        Thread w = new Thread(() -> handle(sock), "worker-" + sock.getPort());
	        w.setDaemon(true);
	        w.start();
	      }

	    } catch (BindException be) {
	      System.err.println("Port " + configuredPort + " is already in use.");
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }
  private SSLServerSocketFactory createSSLServerSocketFactory(String keystore, String pwd) throws Exception {
	    KeyStore ks = KeyStore.getInstance("JKS");
	    try (FileInputStream fis = new FileInputStream(keystore)) {
	      ks.load(fis, pwd.toCharArray());
	    }

	    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
	    kmf.init(ks, pwd.toCharArray());

	    SSLContext sc = SSLContext.getInstance("TLS");
	    sc.init(kmf.getKeyManagers(), null, null);

	    return sc.getServerSocketFactory();
	  }


  private void handle(Socket sock) {
	    try (sock) {
	      InputStream in = sock.getInputStream();
	      OutputStream out = sock.getOutputStream();
	      while (true) {
	        byte[] hdr = readHeaders(in);
	        if (hdr == null) break;

	        ParsedRequest pr = parse(hdr);
	        if (pr.bad) { sendError(out, 400, "Bad Request"); continue; }
	        if (!"HTTP/1.1".equals(pr.protocol)) { sendError(out, 505, "HTTP Version Not Supported"); continue; }

	        byte[] body = (pr.contentLength > 0) ? readBody(in, pr.contentLength) : new byte[0];

	        Match m = matchRoute(pr.method, pr.url);
	        if (m != null) {
	          ResponseImpl res = new ResponseImpl(out);
	          Map<String,String> qparams = parseQueryParams(pr.rawQuery, body, pr.headers.get("content-type"));

	          RequestImpl req = new RequestImpl(
	                  pr.method, pr.url, pr.protocol, pr.headers, qparams, m.params,
	                  (InetSocketAddress) sock.getRemoteSocketAddress(), body, this);

	          try {
	            Object ret = m.handler.handle(req, res);
	            if (res.isCommitted()) break;

	            byte[] payload = res.resolveBody(ret);
	            int len = (payload == null) ? 0 : payload.length;
	            res.writeBufferedHeaders(len);

	            if (!"HEAD".equals(pr.method) && len > 0) {
	              out.write(payload);
	              out.flush();
	            }
	          } catch (Throwable t) {
	            if (!res.isCommitted()) sendError(out, 500, "Internal Server Error");
	            break;
	          }
	          continue;
	        }

	        if ("GET".equals(pr.method) || "HEAD".equals(pr.method)) {
	          if (staticRoot == null) { sendError(out, 404, "Not Found"); continue; }
	          serveStatic(out, staticRoot, pr.url, "GET".equals(pr.method));
	        } else {
	          sendError(out, 405, "Method Not Allowed");
	        }
	      }
	    } catch (IOException ignore) {}
	  }
  private static final class ParsedRequest {
    String method, url, protocol, rawQuery;
    Map<String,String> headers = new HashMap<>();
    int contentLength = 0; boolean bad = false;
  }

  private static byte[] readHeaders(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream(1024);
    int state = 0;
    while (true) {
      int b = in.read();
      if (b == -1) { if (buf.size()==0) return null; throw new EOFException(); }
      buf.write(b);
      switch (state) {
        case 0: state = (b=='\r')?1:0; break;
        case 1: state = (b=='\n')?2:0; break;
        case 2: state = (b=='\r')?3:0; break;
        case 3: if (b=='\n') return buf.toByteArray(); state=0; break;
      }
    }
  }

  private static ParsedRequest parse(byte[] headerBytes) throws IOException {
    ParsedRequest r = new ParsedRequest();
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(headerBytes), StandardCharsets.UTF_8))) {
      String start = br.readLine();
      if (start == null) { r.bad = true; return r; }
      String[] parts = start.split(" ");
      if (parts.length != 3) { r.bad = true; return r; }
      r.method = parts[0]; String rawUrl = parts[1]; r.protocol = parts[2];

      int q = rawUrl.indexOf('?');
      if (q >= 0) { r.url = rawUrl.substring(0, q); r.rawQuery = (q+1<rawUrl.length()) ? rawUrl.substring(q+1) : ""; }
      else { r.url = rawUrl; r.rawQuery = null; }

      String line; boolean sawHost = false;
      while ((line = br.readLine()) != null && line.length() > 0) {
        int idx = line.indexOf(':'); if (idx <= 0) continue;
        String name = line.substring(0, idx).trim().toLowerCase(Locale.ROOT);
        String val  = line.substring(idx + 1).trim();
        r.headers.put(name, val);
        if ("host".equals(name)) sawHost = true;
        else if ("content-length".equals(name)) {
          try { r.contentLength = Integer.parseInt(val); if (r.contentLength < 0) r.bad = true; }
          catch (NumberFormatException nfe) { r.bad = true; }
        }
      }
      if (!sawHost || r.method==null || r.url==null || r.protocol==null) r.bad = true;
      if (!r.url.startsWith("/")) r.bad = true;
    }
    return r;
  }

  private static byte[] readBody(InputStream in, int n) throws IOException {
    byte[] out = new byte[n]; int off = 0;
    while (off < n) {
      int k = in.read(out, off, n - off);
      if (k == -1) throw new EOFException();
      off += k;
    }
    return out;
  }

  private static final class Match { final Route handler; final Map<String,String> params;
    Match(Route h, Map<String,String> p){ handler=h; params=p; } }

  private Match matchRoute(String method, String url) {
    synchronized (routes) {
      for (RouteEntry e : routes) {
        if (!method.equals(e.method)) continue;
        Map<String,String> p = new HashMap<>();
        if (pathMatches(e.pattern, url, p)) return new Match(e.handler, p);
      }
    }
    return null;
  }

  static boolean pathMatches(String pattern, String url, Map<String,String> out) {
    String[] a = pattern.split("/", -1), b = url.split("/", -1);
    if (a.length != b.length) return false;
    for (int i=0;i<a.length;i++){
      if (a[i].startsWith(":")) out.put(a[i].substring(1), urlDecode(b[i]));
      else if (!Objects.equals(a[i], b[i])) return false;
    }
    return true;
  }

  private static Map<String,String> parseQueryParams(String rawQuery, byte[] body, String ctype) {
    Map<String,String> m = new HashMap<>();
    if (rawQuery != null && !rawQuery.isEmpty()) addPairs(m, rawQuery);
    if (ctype != null &&
        ctype.toLowerCase(Locale.ROOT).startsWith("application/x-www-form-urlencoded") &&
        body != null && body.length > 0) {
      addPairs(m, new String(body, StandardCharsets.UTF_8));
    }
    return m;
  }

  private static void addPairs(Map<String,String> m, String s) {
    for (String kv : s.split("&")) {
      if (kv.isEmpty()) continue;
      int eq = kv.indexOf('=');
      String k = urlDecode(eq < 0 ? kv : kv.substring(0, eq));
      String v = urlDecode(eq < 0 ? "" : kv.substring(eq + 1));
      m.merge(k, v, (a, b) -> a + "," + b);
    }
  }

  private static String urlDecode(String x) {
    try { return java.net.URLDecoder.decode(x, StandardCharsets.UTF_8); }
    catch (Exception e){ return x; }
  }

  private static void serveStatic(OutputStream out, Path root, String urlPath, boolean sendBody) throws IOException {
    if (urlPath.contains("..")) { sendError(out, 403, "Forbidden"); return; }
    String sub = urlPath.startsWith("/") ? urlPath.substring(1) : urlPath;
    Path target = root.resolve(sub).normalize();
    if (!target.startsWith(root)) { sendError(out, 403, "Forbidden"); return; }
    if (!Files.exists(target)) { sendError(out, 404, "Not Found"); return; }
    if (!Files.isReadable(target)) { sendError(out, 403, "Forbidden"); return; }
    long len = Files.size(target);
    String ctype = contentType(target.getFileName().toString());

    PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)), false);
    pw.print("HTTP/1.1 200 OK\r\n");
    pw.print("Server: CIS5550-WS\r\n");
    pw.print("Content-Type: " + ctype + "\r\n");
    pw.print("Content-Length: " + len + "\r\n");
    pw.print("\r\n");
    pw.flush();

    if (sendBody) {
      try (InputStream fis = Files.newInputStream(target)) {
        fis.transferTo(out);
      }
      out.flush();
    }
  }

  private static void sendError(OutputStream out, int code, String text) throws IOException {
    byte[] body = (code + " " + text + "\n").getBytes(StandardCharsets.UTF_8);
    PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)), false);
    pw.print("HTTP/1.1 " + code + " " + text + "\r\n");
    pw.print("Server: CIS5550-WS\r\n");
    pw.print("Content-Type: text/plain\r\n");
    pw.print("Content-Length: " + body.length + "\r\n");
    pw.print("\r\n");
    pw.flush();
    out.write(body);
    out.flush();
  }

  private static String contentType(String name) {
    String n = name.toLowerCase(Locale.ROOT);
    if (n.endsWith(".txt"))  return "text/plain";
    if (n.endsWith(".html")) return "text/html";
    if (n.endsWith(".jpg") || n.endsWith(".jpeg")) return "image/jpeg";
    return "application/octet-stream";
  }
}

