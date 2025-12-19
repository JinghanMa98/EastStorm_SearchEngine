package cis5550.flame;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Vector;

import cis5550.kvs.KVSClient;

/**
 * In-memory RDD used by the HW7 tests.
 * Backed by a List<String>. saveAsTable() writes to KVS when requested.
 */
class FlameRDDImpl implements FlameRDD {
  private List<String> data;
  private boolean destroyed = false;

  FlameRDDImpl(List<String> data) {
    this.data = data;
  }

  private void checkAlive() throws Exception {
    if (destroyed) throw new Exception("RDD has been destroyed");
  }

  @Override
  public int count() throws Exception {
    checkAlive();
    return data.size();
  }

  @Override
  public void saveAsTable(String tableName) throws Exception {
    checkAlive();
    // Write each element as its own row, column "value"
    // The key can be anything; we just use an index and element to avoid accidental overwrite.
    KVSClient kvs = Coordinator.kvs;
    int i = 0;
    for (String s : data) {
      String key = "k" + (i++) + "-" + Integer.toHexString(s.hashCode());
      kvs.put(tableName, key, "value", s.getBytes("UTF-8"));
    }
  }

  @Override
  public FlameRDD distinct() throws Exception {
    checkAlive();
    LinkedHashSet<String> set = new LinkedHashSet<>(data);
    return new FlameRDDImpl(new ArrayList<>(set));
  }

  @Override
  public void destroy() throws Exception {
    data = null;
    destroyed = true;
  }

  @Override
  public Vector<String> take(int num) throws Exception {
    checkAlive();
    int n = Math.min(num, data.size());
    Vector<String> v = new Vector<>(n);
    for (int i = 0; i < n; i++) v.add(data.get(i));
    return v;
  }

  @Override
  public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
    checkAlive();
    String acc = zeroElement;
    for (String s : data) acc = lambda.op(acc, s);
    return acc;
  }

  @Override
  public List<String> collect() throws Exception {
    checkAlive();
    return new ArrayList<>(data);
  }

  @Override
  public FlameRDD flatMap(StringToIterable lambda) throws Exception {
    checkAlive();
    ArrayList<String> out = new ArrayList<>();
    for (String s : data) {
      Iterable<String> it = (lambda == null) ? null : lambda.op(s);
      if (it == null) continue;
      for (String x : it) out.add(x);
    }
    return new FlameRDDImpl(out);
  }

  @Override
  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
    checkAlive();
    ArrayList<FlamePair> out = new ArrayList<>();
    for (String s : data) {
      Iterable<FlamePair> it = (lambda == null) ? null : lambda.op(s);
      if (it == null) continue;
      for (FlamePair p : it) out.add(p);
    }
    return new FlamePairRDDImpl(out);
  }

  @Override
  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
    checkAlive();
    ArrayList<FlamePair> out = new ArrayList<>();
    for (String s : data) {
      FlamePair p = (lambda == null) ? null : lambda.op(s);
      if (p != null) out.add(p);
    }
    return new FlamePairRDDImpl(out);
  }

  // ---- extra-credit methods (not needed for HW7 basic tests) ----

  @Override
  public FlameRDD intersection(FlameRDD r) throws Exception {
    checkAlive();
    if (!(r instanceof FlameRDDImpl)) return null; // keep spec: return null if not implemented
    FlameRDDImpl other = (FlameRDDImpl) r;
    java.util.HashSet<String> set1 = new java.util.HashSet<>(this.data);
    java.util.HashSet<String> set2 = new java.util.HashSet<>(other.data);
    set1.retainAll(set2);
    // unique only once (set semantics)
    return new FlameRDDImpl(new ArrayList<>(set1));
  }

  @Override
  public FlameRDD sample(double f) throws Exception {
    checkAlive();
    java.util.Random rand = new java.util.Random();
    ArrayList<String> out = new ArrayList<>();
    for (String s : data) if (rand.nextDouble() < f) out.add(s);
    return new FlameRDDImpl(out);
  }

  @Override
  public FlamePairRDD groupBy(StringToString lambda) throws Exception {
    checkAlive();
    if (lambda == null) return new FlamePairRDDImpl(new ArrayList<>());
    java.util.Map<String, java.util.List<String>> groups = new java.util.HashMap<>();
    for (String s : data) {
      String k = lambda.op(s);
      if (k == null) continue;
      groups.computeIfAbsent(k, kk -> new java.util.ArrayList<>()).add(s);
    }
    ArrayList<FlamePair> out = new ArrayList<>();
    for (var e : groups.entrySet()) {
      String joined = String.join(",", e.getValue());
      out.add(new FlamePair(e.getKey(), joined));
    }
    return new FlamePairRDDImpl(out);
  }

  @Override
  public FlameRDD filter(StringToBoolean lambda) throws Exception {
    checkAlive();
    ArrayList<String> out = new ArrayList<>();
    if (lambda != null) {
      for (String s : data) {
        if (lambda.op(s)) out.add(s);
      }
    }
    return new FlameRDDImpl(out);
  }

  @Override
  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
    checkAlive();
    Iterator<String> it = data.iterator();
    Iterator<String> outIt = (lambda == null) ? java.util.List.<String>of().iterator() : lambda.op(it);
    ArrayList<String> out = new ArrayList<>();
    if (outIt != null) while (outIt.hasNext()) out.add(outIt.next());
    return new FlameRDDImpl(out);
  }
}
