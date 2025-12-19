package cis5550.flame;

import java.util.ArrayList;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

class FlameContextImpl implements FlameContext {
  private final String jarName;
  private final StringBuilder out = new StringBuilder();
  private int concurrencyLevel = 1;

  FlameContextImpl(String jarName) { this.jarName = jarName; }

  @Override public KVSClient getKVS() { return Coordinator.kvs; }

  @Override public void output(String s) {
    if (s == null) return;
    synchronized (out) { if (out.length() > 0) out.append('\n'); out.append(s); }
  }

  String outputOrDefault() {
    synchronized (out) { return (out.length()==0) ? "(job produced no output)" : out.toString(); }
  }

  @Override public FlameRDD parallelize(List<String> list) { return new FlameRDDImpl(new ArrayList<>(list)); }

  @Override public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
    List<String> result = new ArrayList<>();
    for (java.util.Iterator<Row> it = Coordinator.kvs.scan(tableName, null, null); it.hasNext();) {
      Row r = it.next();
      String s = (lambda == null) ? null : lambda.op(r);
      if (s != null) result.add(s);
    }
    return new FlameRDDImpl(result);
  }

  @Override public void setConcurrencyLevel(int k) { concurrencyLevel = Math.max(1, k); }
}

