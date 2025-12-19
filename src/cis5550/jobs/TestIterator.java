package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.*;
import java.util.*;

public class TestIterator {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        ctx.output("Testing iterator on pt-crawl...");
        
        KVSClient kvs = ctx.getKVS();
        
        // Get count (this works)
        int totalCount = kvs.count("pt-crawl");
        ctx.output("Total count from kvs.count(): " + totalCount);
        
        // Try to scan and count manually
        ctx.output("Creating iterator...");
        Iterator<Row> it = kvs.scan("pt-crawl");
        ctx.output("Iterator created, checking hasNext()...");
        
        boolean hasData = it.hasNext();
        ctx.output("hasNext() returned: " + hasData);
        
        if (!hasData) {
            ctx.output("ERROR: Iterator has no data!");
            ctx.output("This means openConnectionAndFill() failed or returned no rows.");
            return;
        }
        
        ctx.output("Starting to iterate...");
        int scannedCount = 0;
        int withUrlCount = 0;
        int withPageCount = 0;
        int withTitleCount = 0;
        
        while (it.hasNext()) {
            Row row = null;
            try {
                row = it.next();
            } catch (Exception e) {
                ctx.output("ERROR calling next(): " + e.getMessage());
                e.printStackTrace();
                break;
            }
            if (row == null) {
                ctx.output("WARNING: got null row at position " + scannedCount);
                continue;
            }
            
            scannedCount++;
            if (row.get("url") != null) withUrlCount++;
            if (row.get("page") != null) withPageCount++;
            if (row.get("title") != null) withTitleCount++;
            
            if (scannedCount % 10000 == 0) {
                ctx.output("  Scanned " + scannedCount + " rows...");
            }
        }
        
        ctx.output("Scan complete!");
        ctx.output("Total rows scanned: " + scannedCount);
        ctx.output("Rows with url: " + withUrlCount);
        ctx.output("Rows with page: " + withPageCount);
        ctx.output("Rows with title: " + withTitleCount);
        ctx.output("");
        ctx.output("Discrepancy: " + (totalCount - scannedCount) + " rows not returned by iterator");
    }
}

