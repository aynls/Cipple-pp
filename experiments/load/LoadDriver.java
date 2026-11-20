// Second-wave Java load driver: same pinned workload as C++ Python wire driver.
// Baseline: Ripple @ 468e9cb. Observation boundary: t_publish immediately before
// pub.put (putAsync writeAndFlush); t_recv inside TimingMessageService.newMessage
// AFTER delegate insert returns (i.e. after SQLite insert, before SyncResponse send).
// This replaces smoke's 200 ms poll with an exact insert hook and preserves behavior
// (transparent Storage/MessageService delegation; Sync+Dispatch both funnel through
// MessageService.newMessage, so fanout paths share the same boundary).
// Usage:
//   java LoadDriver <nodesCsv> <app> <key> <dbDir> <outJson> <numSubs> <warmupOps>
//       <measOps> <gapMs> <deadlineMs> <valueBytes> <t0ms>
// Raw samples -> <outJson without .json>.jsonl  (one JSON per seq x subscriber)
// Summary    -> <outJson>
import ripple.client.RippleClient;
import ripple.client.core.NodeSelector;
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.IncrementalUpdateMessage;
import ripple.common.entity.UpdateMessage;
import ripple.common.storage.AckService;
import ripple.common.storage.ItemService;
import ripple.common.storage.MessageService;
import ripple.common.storage.RecycleStrategy;
import ripple.common.storage.Storage;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

public class LoadDriver {

    static class RecvRecord {
        String msgUuid;
        long tRecvMs;
        long tRecvNano;
    }

    /** Transparent MessageService: delegates everything, timestamps successful inserts. */
    static class TimingMessageService implements MessageService {
        final MessageService delegate;
        final int subIndex;
        final ConcurrentHashMap<Integer, RecvRecord> recv;

        TimingMessageService(MessageService delegate, int subIndex, ConcurrentHashMap<Integer, RecvRecord> recv) {
            this.delegate = delegate;
            this.subIndex = subIndex;
            this.recv = recv;
        }

        static Integer parseSeq(AbstractMessage m) {
            try {
                String v = null;
                if (m instanceof UpdateMessage) v = ((UpdateMessage) m).getValue();
                else if (m instanceof IncrementalUpdateMessage) v = ((IncrementalUpdateMessage) m).getValue();
                if (v == null) return null;
                String[] p = v.split(" ", 4);
                if (p.length < 2) return null;
                return Integer.parseInt(p[1]);
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public boolean newMessage(AbstractMessage message) {
            boolean ok = delegate.newMessage(message);
            if (ok) {
                Integer seq = parseSeq(message);
                if (seq != null && !recv.containsKey(seq)) {
                    RecvRecord r = new RecvRecord();
                    r.msgUuid = message.getUuid().toString();
                    r.tRecvMs = System.currentTimeMillis();
                    r.tRecvNano = System.nanoTime();
                    recv.putIfAbsent(seq, r);
                }
            }
            return ok;
        }

        @Override public boolean exist(UUID u) { return delegate.exist(u); }
        @Override public AbstractMessage getMessageByUuid(UUID u) { return delegate.getMessageByUuid(u); }
        @Override public List<AbstractMessage> findMessages(String a, String k) { return delegate.findMessages(a, k); }
    }

    /** Transparent Storage: only getMessageService() is decorated. */
    static class TimingStorage implements Storage {
        final Storage delegate;
        final TimingMessageService timing;
        TimingStorage(Storage delegate, int subIndex, ConcurrentHashMap<Integer, RecvRecord> recv) {
            this.delegate = delegate;
            this.timing = new TimingMessageService(delegate.getMessageService(), subIndex, recv);
        }
        @Override public String getLocation() { return delegate.getLocation(); }
        @Override public ItemService getItemService() { return delegate.getItemService(); }
        @Override public MessageService getMessageService() { return timing; }
        @Override public AckService getAckService() { return delegate.getAckService(); }
        @Override public RecycleStrategy getRecycleStrategy() { return delegate.getRecycleStrategy(); }
        @Override public void close() { delegate.close(); }
    }

    static String pad(int n) {
        if (n <= 0) return "";
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append('x');
        return sb.toString();
    }

    static double pct(List<Double> sorted, double q) {
        if (sorted.isEmpty()) return -1;
        int idx = (int) Math.ceil(q * sorted.size()) - 1;
        if (idx < 0) idx = 0;
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted.get(idx);
    }

    static String q(String s) { return "\"" + s.replace("\\", "\\\\").replace("\"", "'") + "\""; }

    public static void main(String[] args) throws Exception {
        String nodesCsv = args[0];
        String app = args[1];
        String key = args[2];
        String dbDir = args[3];
        String outJson = args[4];
        int numSubs = Integer.parseInt(args[5]);
        int warmupOps = Integer.parseInt(args[6]);
        int measOps = Integer.parseInt(args[7]);
        long gapMs = Long.parseLong(args[8]);
        long deadlineMs = Long.parseLong(args[9]);
        int valueBytes = Integer.parseInt(args[10]);
        long t0 = Long.parseLong(args[11]);
        Files.createDirectories(Paths.get(dbDir));

        List<ripple.common.entity.NodeMetadata> nodes = new ArrayList<>();
        for (String item : nodesCsv.split(",")) {
            String[] p = item.split(":");
            nodes.add(new ripple.common.entity.NodeMetadata(Integer.parseInt(p[0]), p[1], Integer.parseInt(p[2])));
        }

        List<RippleClient> subs = new ArrayList<>();
        List<ConcurrentHashMap<Integer, RecvRecord>> recvMaps = new ArrayList<>();
        // Deterministic placement by node index (matches Python wire driver):
        // sub i pins to nodes[i % N], publisher pins to nodes[0] (node 1).
        // Native Java NodeSelector lambda per client (no hashing randomness).
        for (int i = 0; i < numSubs; i++) {
            final int subIdx = i;
            NodeSelector subSel = (applicationName, k, nodeList) ->
                    nodeList.get(subIdx % nodeList.size());
            RippleClient c = new RippleClient(nodes, subSel,
                    dbDir + "/sub" + i + ".db", "127.0.0.1");
            ConcurrentHashMap<Integer, RecvRecord> recv = new ConcurrentHashMap<>();
            c.setStorage(new TimingStorage(c.getStorage(), i, recv));
            recvMaps.add(recv);
            subs.add(c);
        }
        NodeSelector pubSel = (applicationName, k, nodeList) -> nodeList.get(0);
        RippleClient pub = new RippleClient(nodes, pubSel,
                dbDir + "/pub.db", "127.0.0.1");

        List<String> errors = new ArrayList<>();
        Map<Integer, Long> tPub = new ConcurrentHashMap<>();
        Map<Integer, Long> tPubNano = new ConcurrentHashMap<>();
        long wallStart = 0, wallEnd = 0, lastPub = 0, lastRecv = 0;
        try {
            for (RippleClient c : subs) c.start();
            pub.start();
            for (int i = 0; i < numSubs; i++) subs.get(i).subscribe(app, key);
            Thread.sleep(2000); // async SUBSCRIBE settle (recorded config, not a latency metric)

            // Warm-up (not measured; seq offset avoids collision with measurement seqs).
            for (int w = 0; w < warmupOps; w++) {
                int seq = 1000000 + w;
                long tp = System.currentTimeMillis();
                String prefix = t0 + " " + seq + " " + tp + " ";
                String v = prefix + pad(Math.max(0, valueBytes - prefix.length()));
                pub.put(app, key, v);
                if (gapMs > 0) Thread.sleep(gapMs);
                long end = System.currentTimeMillis() + Math.min(deadlineMs, 5000);
                while (System.currentTimeMillis() < end) {
                    boolean all = true;
                    for (ConcurrentHashMap<Integer, RecvRecord> m : recvMaps) {
                        if (!m.containsKey(seq)) { all = false; break; }
                    }
                    if (all) break;
                    Thread.sleep(5);
                }
            }

            wallStart = System.currentTimeMillis();
            long schedule = System.nanoTime();
            for (int seq = 0; seq < measOps; seq++) {
                long planned = schedule + seq * gapMs * 1000000;
                long remaining;
                while ((remaining = planned - System.nanoTime()) > 0) {
                    LockSupport.parkNanos(remaining);
                }
                long tp = System.currentTimeMillis();
                tPub.put(seq, tp);
                String prefix = t0 + " " + seq + " " + tp + " ";
                String v = prefix + pad(Math.max(0, valueBytes - prefix.length()));
                lastPub = tp;
                tPubNano.put(seq, System.nanoTime());
                pub.put(app, key, v);
            }
            long drainEnd = System.currentTimeMillis() + deadlineMs;
            boolean drained = false;
            while (System.currentTimeMillis() < drainEnd) {
                boolean all = true;
                for (int seq = 0; seq < measOps; seq++) {
                    for (ConcurrentHashMap<Integer, RecvRecord> m : recvMaps) {
                        if (!m.containsKey(seq)) { all = false; break; }
                    }
                    if (!all) break;
                }
                if (all) { drained = true; break; }
                Thread.sleep(5);
            }
            wallEnd = System.currentTimeMillis();
            if (!drained) errors.add("drain_timeout: not all (seq x sub) received within " + deadlineMs + "ms");
            for (ConcurrentHashMap<Integer, RecvRecord> m : recvMaps) {
                for (Map.Entry<Integer, RecvRecord> row : m.entrySet()) {
                    if (row.getKey() < measOps && row.getValue().tRecvMs > lastRecv) lastRecv = row.getValue().tRecvMs;
                }
            }
        } catch (Exception e) {
            errors.add("exception: " + e);
        }

        // Freeze the completed delivery set before serialization. The driver's
        // disposable JVM owns all clients and exits after flushing the report.
        long cutoff = System.nanoTime();
        for (int s = 0; s < numSubs; s++) {
            ConcurrentHashMap<Integer, RecvRecord> frozen = new ConcurrentHashMap<>();
            recvMaps.get(s).forEach((seq, row) -> {
                if (row.tRecvNano <= cutoff) frozen.put(seq, row);
            });
            recvMaps.set(s, frozen);
        }

        // Raw samples -> sibling .jsonl
        String rawPath = outJson.endsWith(".json") ? outJson.substring(0, outJson.length() - 5) + ".jsonl" : outJson + ".jsonl";
        StringBuilder raw = new StringBuilder();
        List<Double> allLat = new ArrayList<>();
        List<List<Double>> perSubLat = new ArrayList<>();
        for (int i = 0; i < numSubs; i++) perSubLat.add(new ArrayList<>());
        int received = 0;
        int total = measOps * numSubs;
        for (int seq = 0; seq < measOps; seq++) {
            Long tp = tPub.get(seq);
            for (int s = 0; s < numSubs; s++) {
                RecvRecord r = recvMaps.get(s).get(seq);
                String line;
                if (r != null && tp != null) {
                    double lat = (r.tRecvNano - tPubNano.get(seq)) / 1000000.0;
                    allLat.add(lat);
                    perSubLat.get(s).add(lat);
                    received++;
                    line = "{\"seq\":" + seq + ",\"subscriber\":" + s + ",\"msg_uuid\":\"" + r.msgUuid
                            + "\",\"t_publish_ms\":" + tp + ",\"t_recv_ms\":" + r.tRecvMs
                            + ",\"latency_ms\":" + lat + ",\"result\":\"ok\",\"error\":\"\"}\n";
                } else {
                    line = "{\"seq\":" + seq + ",\"subscriber\":" + s + ",\"msg_uuid\":\"\","
                            + "\"t_publish_ms\":" + (tp == null ? -1 : tp) + ",\"t_recv_ms\":-1,"
                            + "\"latency_ms\":-1,\"result\":\"missing\",\"error\":\"not_received_within_deadline\"}\n";
                }
                raw.append(line);
            }
        }
        Files.write(Paths.get(rawPath), raw.toString().getBytes("UTF-8"));
        Collections.sort(allLat);
        for (List<Double> l : perSubLat) Collections.sort(l);

        double wallSec = Math.max(1, wallEnd - wallStart) / 1000.0;
        int completedOps = 0;
        for (int seq = 0; seq < measOps; seq++) {
            boolean ok = true;
            for (int s = 0; s < numSubs; s++) if (!recvMaps.get(s).containsKey(seq)) { ok = false; break; }
            if (ok) completedOps++;
        }
        double goodput = completedOps / wallSec;
        double coverage = total == 0 ? 0 : (received * 100.0 / total);
        long drainMs = (lastRecv > 0 && lastPub > 0) ? (lastRecv - lastPub) : -1;

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"engine\": \"java-ripple-native-client\",\n");
        sb.append("  \"observation\": \"t_publish=before putAsync; t_recv=after subscriber MessageService.newMessage insert (before SyncResponse). No 200ms poll in latency path. Native RippleClient with embedded SQLite; compare against C++ wire-harness profiles explicitly labelled.\",\n");
        sb.append("  \"nodes\": ").append(q(nodesCsv)).append(",\n");
        sb.append("  \"placement\": \"index-mod: sub i -> nodes[i % N], pub -> nodes[0] (node 1)\",\n");
        // Record resolved homes for auditability (sub i -> nodes[i % N], pub -> nodes[0]).
        sb.append("  \"sub_homes\": [");
        for (int s = 0; s < numSubs; s++) {
            if (s > 0) sb.append(", ");
            ripple.common.entity.NodeMetadata hn = nodes.get(s % nodes.size());
            sb.append(q(hn.getId() + ":" + hn.getAddress() + ":" + hn.getPort()));
        }
        sb.append("],\n");
        {
            ripple.common.entity.NodeMetadata hp = nodes.get(0);
            sb.append("  \"pub_home\": ").append(q(hp.getId() + ":" + hp.getAddress() + ":" + hp.getPort())).append(",\n");
        }
        sb.append("  \"app\": ").append(q(app)).append(", \"key\": ").append(q(key)).append(",\n");
        sb.append("  \"num_subscribers\": ").append(numSubs).append(",\n");
        sb.append("  \"warmup_ops\": ").append(warmupOps).append(", \"meas_ops\": ").append(measOps).append(",\n");
        sb.append("  \"gap_ms\": ").append(gapMs).append(", \"deadline_ms\": ").append(deadlineMs).append(",\n");
        sb.append("  \"value_bytes\": ").append(valueBytes).append(", \"t0_ms\": ").append(t0).append(",\n");
        sb.append("  \"subscribe_settle_ms\": 2000,\n");
        sb.append("  \"java_version\": ").append(q(System.getProperty("java.version") + " " + System.getProperty("java.vendor"))).append(",\n");
        sb.append("  \"wall_ms\": ").append(wallEnd - wallStart).append(",\n");
        sb.append("  \"measurement_start_ms\": ").append(wallStart).append(", \"measurement_end_ms\": ").append(wallEnd).append(",\n");
        long sendSpan = tPubNano.isEmpty() ? 0 : Collections.max(tPubNano.values()) - Collections.min(tPubNano.values());
        sb.append("  \"send_span_ms\": ").append(sendSpan / 1000000.0).append(",\n");
        sb.append("  \"completed_ops\": ").append(completedOps).append(",\n");
        sb.append("  \"goodput_ops\": ").append(String.format(Locale.ROOT, "%.3f", goodput)).append(",\n");
        sb.append("  \"coverage_pct\": ").append(String.format(Locale.ROOT, "%.3f", coverage)).append(",\n");
        sb.append("  \"received_pairs\": ").append(received).append(", \"total_pairs\": ").append(total).append(",\n");
        sb.append("  \"drain_ms\": ").append(drainMs).append(",\n");
        sb.append("  \"p50_ms\": ").append(pct(allLat, 0.50)).append(", \"p95_ms\": ").append(pct(allLat, 0.95)).append(", \"p99_ms\": ").append(pct(allLat, 0.99)).append(",\n");
        sb.append("  \"per_subscriber\": [");
        for (int s = 0; s < numSubs; s++) {
            if (s > 0) sb.append(", ");
            List<Double> l = perSubLat.get(s);
            sb.append("{\"subscriber\":").append(s).append(",\"n\":").append(l.size())
              .append(",\"p50_ms\":").append(pct(l, 0.50)).append(",\"p95_ms\":").append(pct(l, 0.95))
              .append(",\"p99_ms\":").append(pct(l, 0.99)).append("}");
        }
        sb.append("],\n");
        sb.append("  \"errors\": [");
        for (int i = 0; i < errors.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(q(errors.get(i)));
        }
        sb.append("],\n");
        sb.append("  \"raw_jsonl\": ").append(q(rawPath)).append("\n}\n");
        Files.write(Paths.get(outJson), sb.toString().getBytes("UTF-8"));
        System.out.println(sb.toString());
        System.exit(errors.isEmpty() && received == total ? 0 : 1);
    }
}
