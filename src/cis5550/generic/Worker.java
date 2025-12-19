package cis5550.generic;

import cis5550.tools.HTTP;

/** Common worker utilities (ping thread). */
public class Worker {
  /** Ping coordinator every 5 seconds. */
  public static Thread startPingThread(String coordHostPort, String id, int port) {
    Thread t = new Thread(() -> {
      final String url = "http://" + coordHostPort + "/ping?id=" + id + "&port=" + port;
      while (true) {
        try { HTTP.doRequest("GET", url, null); } catch (Exception ignored) {}
        try { Thread.sleep(5000); } catch (InterruptedException ie) { return; }
      }
    }, "PingThread");
    t.setDaemon(true);
    t.start();
    return t;
  }
}
