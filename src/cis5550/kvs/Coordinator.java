package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Minimal coordinator: workers PUT /register/:id (body=host:port); GET /workers list */
public class Coordinator {
  private static final Map<String,String> workers = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: java cis5550.kvs.Coordinator <port>");
      System.exit(1);
    }
    int portNum = Integer.parseInt(args[0]);
    port(portNum);

    // Worker registration
    put("/register/:id", (req, res) -> {
      String id = req.params("id");
      String addr = req.body().trim(); // e.g., localhost:8001
      if (id == null || id.isEmpty() || addr.isEmpty()) {
        res.status(400, "Bad Request");
        return "Missing id or address";
      }
      workers.put(id, addr);
      return "OK";
    });

    // List workers (format expected by KVSClient)
    get("/workers", (req, res) -> {
      List<String> ids = new ArrayList<>(workers.keySet());
      Collections.sort(ids);
      StringBuilder sb = new StringBuilder();
      sb.append(ids.size()).append('\n');
      for (String id : ids) {
        sb.append(id).append(',').append(workers.get(id)).append('\n');
      }
      res.type("text/plain");
      return sb.toString();
    });
  }
}

