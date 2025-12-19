package cis5550.webserver;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

class ResponseImpl implements Response {
  private final OutputStream out;
  private final List<String[]> headers = new ArrayList<>();
  private int statusCode = 200;
  private String reasonPhrase = "OK";
  private String contentType = "text/html";
  private byte[] body = null;
  private boolean committed = false;

  ResponseImpl(OutputStream out){ this.out = out; }

  @Override public void body(String s){ if (!committed) body = (s==null)?null:s.getBytes(StandardCharsets.UTF_8); }
  @Override public void bodyAsBytes(byte[] b){ if (!committed) body = b; }
  @Override public void header(String name, String value){ if (!committed) headers.add(new String[]{name, value}); }
  @Override public void type(String t){ if (!committed) contentType = t; }
  @Override public void status(int c, String r){ if (!committed) { statusCode=c; reasonPhrase=r; } }

  @Override
  public void write(byte[] b) throws Exception {
    if (!committed) {
      committed = true;
      PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)), false);
      pw.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
      pw.print("Server: CIS5550-WS\r\n");
      pw.print("Connection: close\r\n");     
      pw.print("Content-Type: " + contentType + "\r\n");
      for (String[] h : headers) pw.print(h[0] + ": " + h[1] + "\r\n");
      pw.print("\r\n");
      pw.flush();
    }
    if (b != null && b.length > 0) { out.write(b); out.flush(); }
  }

  @Override
  public void redirect(String url, int responseCode) {
    if (committed) return;
    if (responseCode < 300 || responseCode > 399) responseCode = 302;
    statusCode = responseCode;
    reasonPhrase = reasonForRedirect(responseCode);
    headers.add(new String[]{"Location", url});
    try {
      writeBufferedHeaders(0);
    } catch (IOException ignored) {}
  }

  @Override
  public void halt(int code, String reason) {
    if (committed) return;
    statusCode = code;
    reasonPhrase = (reason == null ? "" : reason);
    try {
      int len = (body == null) ? 0 : body.length;
      writeBufferedHeaders(len);
      if (len > 0) {
        out.write(body);
        out.flush();
      }
    } catch (IOException ignored) {}
  }
  
  private static String reasonForRedirect(int code) {
	    switch (code) {
	      case 301: return "Moved Permanently";
	      case 302: return "Found";
	      case 303: return "See Other";
	      case 307: return "Temporary Redirect";
	      case 308: return "Permanent Redirect";
	      default:  return "Found";
	    }
	  }


  boolean isCommitted(){ return committed; }

  byte[] resolveBody(Object routeReturn){
    if (committed) return null;
    if (routeReturn != null) return routeReturn.toString().getBytes(StandardCharsets.UTF_8);
    return body;
  }

  void writeBufferedHeaders(int contentLength) throws IOException {
    if (committed) return;
    committed = true;
    PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8)), false);
    pw.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
    pw.print("Server: CIS5550-WS\r\n");
    pw.print("Content-Type: " + contentType + "\r\n");
    pw.print("Content-Length: " + contentLength + "\r\n");
    for (String[] h : headers) pw.print(h[0] + ": " + h[1] + "\r\n");
    pw.print("\r\n");
    pw.flush();
  }
}
