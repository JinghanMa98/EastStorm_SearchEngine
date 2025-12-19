package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.HTTP;
import java.util.*;

/**
 * Custom scanner that properly handles pagination by making multiple requests per worker.
 * This bypasses the broken iterator and scans the entire table.
 */
public class TestCustomScan {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("Testing custom scan on pt-crawl...");
        
        KVSClient kvs = ctx.getKVS();
        
        // Get total count
        int totalCount = kvs.count("pt-crawl");
        ctx.output("Total count from kvs.count(): " + totalCount);
        
        // Get KVS worker addresses
        int numWorkers = kvs.numWorkers();
        ctx.output("Number of KVS workers: " + numWorkers);
        
        int totalScanned = 0;
        int withUrl = 0;
        int withTitle = 0;
        int withPage = 0;
        
        // Scan each worker with pagination
        for (int workerIdx = 0; workerIdx < numWorkers; workerIdx++) {
            String workerAddr = kvs.getWorkerAddress(workerIdx);
            ctx.output("");
            ctx.output("Scanning worker " + (workerIdx + 1) + "/" + numWorkers + " (" + workerAddr + ")...");
            
            // Get worker's row count
            HTTP.Response countResp = HTTP.doRequest("GET", "http://" + workerAddr + "/count/pt-crawl", null);
            int workerCount = Integer.parseInt(new String(countResp.body()).trim());
            ctx.output("  Worker has " + workerCount + " rows");
            
            // Scan this worker's data with pagination
            // We'll make multiple requests, each returning up to 2000 rows
            int workerScanned = 0;
            String lastKey = null;
            String previousKey = null;
            int requestNum = 0;
            int stuckCount = 0;
            
            while (workerScanned < workerCount) {
                requestNum++;
                
                // Build URL with pagination
                String url = "http://" + workerAddr + "/data/pt-crawl";
                if (lastKey != null) {
                    url += "?startRow=" + java.net.URLEncoder.encode(lastKey, "UTF-8");
                }
                
                ctx.output("  Request " + requestNum + ": fetching from " + (lastKey == null ? "beginning" : lastKey) + "...");
                
                // Fetch data
                HTTP.Response resp = HTTP.doRequest("GET", url, null);
                if (resp == null || resp.statusCode() != 200) {
                    ctx.output("  ERROR: Failed to fetch data");
                    break;
                }
                
                // Parse rows using Row.readFrom()
                byte[] body = resp.body();
                java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(body);
                
                int rowsInBatch = 0;
                try {
                    while (true) {
                        Row row = Row.readFrom(bais);
                        if (row == null) break;
                        
                        lastKey = row.key();
                        rowsInBatch++;
                        
                        // Check for columns
                        if (row.get("url") != null) withUrl++;
                        if (row.get("title") != null) withTitle++;
                        if (row.getBytes("page") != null) withPage++;
                    }
                } catch (Exception e) {
                    ctx.output("  Error parsing rows: " + e.getMessage());
                }
                
                workerScanned += rowsInBatch;
                totalScanned += rowsInBatch;
                
                ctx.output("  Got " + rowsInBatch + " rows (total from this worker: " + workerScanned + ")");
                
                // If we got 0 rows, we've reached the end
                if (rowsInBatch == 0) {
                    ctx.output("  Reached end of worker data (0 rows returned)");
                    break;
                }
                
                // Check if we're stuck on the same key (corrupted row)
                if (lastKey != null && lastKey.equals(previousKey)) {
                    stuckCount++;
                    if (stuckCount >= 3) {
                        ctx.output("  WARNING: Stuck on key " + lastKey + ", skipping ahead");
                        // Skip ahead by appending 'z' to the key
                        lastKey = lastKey + "z";
                        stuckCount = 0;
                    }
                } else {
                    stuckCount = 0;
                }
                previousKey = lastKey;
                
                // Safety: don't make too many requests
                if (requestNum > 200) {
                    ctx.output("  WARNING: Too many requests, stopping");
                    break;
                }
            }
            
            ctx.output("  Finished worker: scanned " + workerScanned + " rows");
        }
        
        ctx.output("");
        ctx.output("=== CUSTOM SCAN COMPLETE ===");
        ctx.output("Total rows scanned: " + totalScanned);
        ctx.output("Rows with url: " + withUrl);
        ctx.output("Rows with title: " + withTitle);
        ctx.output("Rows with page: " + withPage);
        ctx.output("Discrepancy: " + (totalCount - totalScanned));
    }
}

