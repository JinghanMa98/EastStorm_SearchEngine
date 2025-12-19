package cis5550.generic;

import static cis5550.webserver.Server.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator {

  /** Info per worker. */
  protected static class WorkerInfo {
    final String id;
    volatile String host;    
    volatile int port;
    volatile long lastPingMs;
    WorkerInfo(String id, String host, int port) {
      this.id = id; this.host = host; this.port = port;
      this.lastPingMs = System.currentTimeMillis();
    }
  }

  /** Active workers by ID. */
  protected static final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
  protected static final long EXPIRE_MS = 15_000; // 15s

  private static void prune() {
    long now = System.currentTimeMillis();
    workers.values().removeIf(w -> (now - w.lastPingMs) > EXPIRE_MS);
  }

  /** Builds the HTML table for GET /. */
  public static String workerTable() {
    prune();
    List<WorkerInfo> list = workers.values().stream()
        .sorted(Comparator.comparing(w -> w.id))
        .toList();

    StringBuilder sb = new StringBuilder();
    sb.append("<table border='1' cellpadding='6' cellspacing='0'>");
    sb.append("<tr><th>ID</th><th>Host</th><th>Port</th><th>Link</th></tr>");
    for (WorkerInfo w : list) {
      sb.append("<tr>")
        .append("<td>").append(w.id).append("</td>")
        .append("<td>").append(w.host).append("</td>")
        .append("<td>").append(w.port).append("</td>")
        .append("<td><a href='http://").append(w.host).append(":").append(w.port)
        .append("/' target='_blank'>http://").append(w.host).append(":").append(w.port)
        .append("/</a></td>")
        .append("</tr>");
    }
    sb.append("</table>");
    return sb.toString();
  }

  public static Vector<String> getWorkers() {
    prune();
    Vector<String> v = new Vector<>();
    workers.values().stream().sorted(Comparator.comparing(w -> w.id))
        .forEach(w -> v.add(w.host + ":" + w.port));
    return v;
  }
  /** For compatibility with Flame (same as workerTable). */
  public static String clientTable() {
    return workerTable();
  }

  /** Defines /ping and /workers routes. */
  public static void registerRoutes() {
    // GET /ping?id=<id>&port=<port>
    get("/ping", (req, res) -> {
      String id = req.queryParams("id");
      String portStr = req.queryParams("port");
      if (id == null || portStr == null) {
        res.status(400, "Missing id or port");
        return "Missing id or port";
      }
      final int port;
      try { port = Integer.parseInt(portStr); }
      catch (NumberFormatException e) {
        res.status(400, "Invalid port");
        return "Invalid port";
      }

      String host = req.ip();
      workers.compute(id, (k, old) -> {
    	  WorkerInfo w = (old == null) ? new WorkerInfo(id, host, port) : old;
    	  w.host = host; 
    	  w.port = port; 
    	  w.lastPingMs = System.currentTimeMillis();
    	  return w;
    	});
      return "OK";
    });

    get("/workers", (req, res) -> {
      prune();
      List<WorkerInfo> list = workers.values().stream()
          .sorted(Comparator.comparing(w -> w.id))
          .toList();
      StringBuilder sb = new StringBuilder();
      sb.append(list.size()).append("\n");
      for (WorkerInfo w : list) {
    	  sb.append(w.id).append(",").append(w.host).append(":").append(w.port).append("\n");

      }
      res.type("text/plain");
      return sb.toString();
    });
  }
}

