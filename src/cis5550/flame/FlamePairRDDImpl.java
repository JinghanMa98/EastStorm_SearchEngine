package cis5550.flame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cis5550.kvs.KVSClient;

class FlamePairRDDImpl implements FlamePairRDD {
  private List<FlamePair> data;
  private boolean destroyed = false;

  FlamePairRDDImpl(List<FlamePair> data) {
    this.data = data;
  }

  private void checkAlive() throws Exception {
    if (destroyed) throw new Exception("PairRDD has been destroyed");
  }

  @Override
  public List<FlamePair> collect() throws Exception {
    checkAlive();
    return new ArrayList<>(data);
  }

  @Override
  public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
    checkAlive();
    Map<String, String> acc = new HashMap<>();
    for (FlamePair p : data) {
      String k = p._1(), v = p._2();
      String prev = acc.getOrDefault(k, zeroElement);
      acc.put(k, lambda.op(prev, v));
    }
    ArrayList<FlamePair> out = new ArrayList<>();
    for (Map.Entry<String, String> e : acc.entrySet()) out.add(new FlamePair(e.getKey(), e.getValue()));
    return new FlamePairRDDImpl(out);
  }

  @Override
  public void saveAsTable(String tableName) throws Exception {
    checkAlive();
    // For each key, write values to distinct columns (v0, v1, ...)
    KVSClient kvs = Coordinator.kvs;
    Map<String, Integer> nextColIdx = new HashMap<>();
    for (FlamePair p : data) {
      String k = p._1(), v = p._2();
      int idx = nextColIdx.getOrDefault(k, 0);
      kvs.put(tableName, k, "v" + idx, v.getBytes("UTF-8"));
      nextColIdx.put(k, idx + 1);
    }
  }

  @Override
  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
    checkAlive();
    ArrayList<String> out = new ArrayList<>();
    for (FlamePair p : data) {
      Iterable<String> it = (lambda == null) ? null : lambda.op(p);
      if (it == null) continue;
      for (String s : it) out.add(s);
    }
    return new FlameRDDImpl(out);
  }

  @Override
  public void destroy() throws Exception {
    data = null;
    destroyed = true;
  }

  @Override
  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
    checkAlive();
    ArrayList<FlamePair> out = new ArrayList<>();
    for (FlamePair p : data) {
      Iterable<FlamePair> it = (lambda == null) ? null : lambda.op(p);
      if (it == null) continue;
      for (FlamePair q : it) out.add(q);
    }
    return new FlamePairRDDImpl(out);
  }

  @Override
  public FlamePairRDD join(FlamePairRDD other) throws Exception {
    checkAlive();
    if (!(other instanceof FlamePairRDDImpl))
      throw new IllegalArgumentException("Join expects compatible implementation");

    FlamePairRDDImpl o = (FlamePairRDDImpl) other;

    // Group values by key for both sides
    Map<String, List<String>> left = new HashMap<>();
    Map<String, List<String>> right = new HashMap<>();
    for (FlamePair p : this.data) {
      left.computeIfAbsent(p._1(), k -> new ArrayList<>()).add(p._2());
    }
    for (FlamePair p : o.data) {
      right.computeIfAbsent(p._1(), k -> new ArrayList<>()).add(p._2());
    }

    ArrayList<FlamePair> out = new ArrayList<>();
    for (String k : left.keySet()) {
      List<String> lv = left.get(k);
      List<String> rv = right.get(k);
      if (rv == null) continue;
      for (String a : lv)
        for (String b : rv)
          out.add(new FlamePair(k, a + "," + b));
    }
    return new FlamePairRDDImpl(out);
  }

  @Override
  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
    checkAlive();
    if (!(other instanceof FlamePairRDDImpl)) return null; // per spec if not implemented
    FlamePairRDDImpl o = (FlamePairRDDImpl) other;

    java.util.Map<String, java.util.List<String>> left = new java.util.HashMap<>();
    java.util.Map<String, java.util.List<String>> right = new java.util.HashMap<>();

    for (FlamePair p : this.data)
      left.computeIfAbsent(p._1(), k -> new java.util.ArrayList<>()).add(p._2());
    for (FlamePair p : o.data)
      right.computeIfAbsent(p._1(), k -> new java.util.ArrayList<>()).add(p._2());

    java.util.Set<String> allKeys = new java.util.HashSet<>();
    allKeys.addAll(left.keySet());
    allKeys.addAll(right.keySet());

    ArrayList<FlamePair> out = new ArrayList<>();
    for (String k : allKeys) {
      java.util.List<String> l = left.getOrDefault(k, java.util.List.of());
      java.util.List<String> r = right.getOrDefault(k, java.util.List.of());
      java.util.ArrayList<String> ls = new java.util.ArrayList<>(l);
      java.util.ArrayList<String> rs = new java.util.ArrayList<>(r);
      java.util.Collections.sort(ls);
      java.util.Collections.sort(rs);
      String lv = "[" + String.join(",", ls) + "]";
      String rv = "[" + String.join(",", rs) + "]";
      out.add(new FlamePair(k, lv + "," + rv));
    }

    return new FlamePairRDDImpl(out);
  }
}
