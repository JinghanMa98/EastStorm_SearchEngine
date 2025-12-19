package cis5550.jobs;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class SearchTest {

    public static void main(String[] args) throws Exception {
        String coord = "localhost:8000";  // ✅ 注意：必须是 127.0.0.1!!
        String key = "way";

        KVSClient kvs = new KVSClient(coord);

        // 取整行
        Row r = kvs.getRow("pt-index", key);
        if (r == null) {
            System.out.println("Row not found.");
            return;
        }

        // 因为 pt-index 每行只有一个列，直接取第一列名
        String col = r.columns().iterator().next();
        String value = r.get(col);

        System.out.println("=== pt-index changed[" + key + "] ===");
        System.out.println("column: " + col);
        System.out.println("value:");
        System.out.println(value);
    }
}


