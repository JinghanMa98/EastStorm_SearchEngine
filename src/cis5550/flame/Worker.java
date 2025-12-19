package cis5550.flame;

import static cis5550.webserver.Server.*;

import java.io.File;
import java.io.FileOutputStream;

import cis5550.tools.Logger;

public class Worker extends cis5550.generic.Worker {

  private static final Logger logger = Logger.getLogger(Worker.class);

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
      System.exit(1);
    }

    final int myPort = Integer.parseInt(args[0]);
    final String coordHostPort = args[1];          
    final String myId = "W" + myPort;              

    // 1) HTTP server
    port(myPort);

    // 2) Register with coordinator
    startPingThread(coordHostPort, myId, myPort);



    // 3) Health check
    get("/", (req, res) -> "OK");

    // 4) Accept JAR uploads from coordinator
    final File myJar = new File("__worker" + myPort + "-current.jar");
    post("/useJAR", (req, res) -> {
      try (FileOutputStream fos = new FileOutputStream(myJar)) {
        fos.write(req.bodyAsBytes());
      }
      logger.info("Updated worker JAR -> " + myJar.getAbsolutePath());
      return "OK";
    });
  }
}

