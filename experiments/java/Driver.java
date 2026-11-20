// Pinned Java baseline E2E driver for Cipple++ experiments.
// Baseline: ISCAS-SSG/Ripple @ 468e9cb (built from `git archive HEAD` in private temp; this repo never modifies Ripple).
// Uses embedded RippleClient pub/sub against standalone ripple-server processes (real multi-process TCP).
// Mirrors workload.json smoke_online_full. DO NOT change semantics without bumping workload.json.
import ripple.client.RippleClient;
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.Constants;
import ripple.common.entity.IncrementalUpdateMessage;
import ripple.common.entity.Item;
import ripple.common.entity.NodeMetadata;
import ripple.common.entity.UpdateMessage;
import ripple.common.hashing.ModHashing;
import ripple.client.core.HashingBasedSelector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Pinned Java baseline end-to-end driver (embedded RippleClient API).
 * Talks TCP to standalone ripple-server processes; proves multi-process path.
 * Usage: java Driver <nodesCsv> <app> <key> <dbDir> <outJson>
 *   nodesCsv: "1:127.0.0.1:13101,2:127.0.0.1:13102"
 */
public class Driver {
    static List<NodeMetadata> parseNodes(String csv) {
        List<NodeMetadata> out = new ArrayList<>();
        for (String item : csv.split(",")) {
            String[] p = item.split(":");
            out.add(new NodeMetadata(Integer.parseInt(p[0]), p[1], Integer.parseInt(p[2])));
        }
        return out;
    }

    static int messageCount(RippleClient c, String app, String key) {
        try {
            List<AbstractMessage> ms = c.getStorage().getMessageService().findMessages(app, key);
            return ms == null ? 0 : ms.size();
        } catch (Exception e) {
            return -1;
        }
    }

    static AbstractMessage latest(RippleClient c, String app, String key) {
        List<AbstractMessage> ms = c.getStorage().getMessageService().findMessages(app, key);
        if (ms == null || ms.isEmpty()) return null;
        AbstractMessage best = ms.get(0);
        for (AbstractMessage m : ms) {
            if (m.getLastUpdate().after(best.getLastUpdate())) best = m;
        }
        return best;
    }

    static boolean waitForMessages(RippleClient c, String app, String key, int want, long timeoutMs) throws Exception {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (messageCount(c, app, key) >= want) return true;
            Thread.sleep(200);
        }
        return messageCount(c, app, key) >= want;
    }

    public static void main(String[] args) throws Exception {
        String nodesCsv = args[0];
        String app = args[1];
        String key = args[2];
        String dbDir = args[3];
        String outJson = args[4];
        Files.createDirectories(Paths.get(dbDir));
        List<NodeMetadata> nodes = parseNodes(nodesCsv);

        List<String> errors = new ArrayList<>();
        List<String> steps = new ArrayList<>();
        boolean ok = true;
        long tSub0 = 0, tPut0 = 0, tDispatchedMs = -1, tGetMs = -1, tIncMs = -1, tDelMs = -1;
        String putValue = null, mergedAfterPut = null, mergedAfterInc = null, mergedAfterDel = "<n/a>";
        int subMsgBefore = 0, subMsgAfterPut = 0, subMsgAfterInc = 0, subMsgAfterDel = 0;

        RippleClient sub = new RippleClient(nodes, new HashingBasedSelector(new ModHashing()),
                dbDir + "/driver-sub.db", "127.0.0.1");
        RippleClient pub = new RippleClient(nodes, new HashingBasedSelector(new ModHashing()),
                dbDir + "/driver-pub.db", "127.0.0.1");
        try {
            sub.start();
            pub.start();
            steps.add("clients_started");
            tSub0 = System.currentTimeMillis();
            sub.subscribe(app, key);
            Thread.sleep(2000); // async SUBSCRIBE round-trip
            steps.add("subscribed");
            subMsgBefore = messageCount(sub, app, key);

            putValue = "smoke-" + UUID.randomUUID();
            long t0 = System.currentTimeMillis();
            tPut0 = t0;
            pub.put(app, key, putValue);
            boolean arrived = waitForMessages(sub, app, key, subMsgBefore + 1, 15000);
            long t1 = System.currentTimeMillis();
            tDispatchedMs = t1 - t0;
            subMsgAfterPut = messageCount(sub, app, key);
            if (!arrived) {
                ok = false;
                errors.add("dispatch_timeout: sub messages stayed at " + subMsgAfterPut);
            } else {
                steps.add("dispatch_observed");
            }

            // Read-your-write on publisher side (GET round-trip then local merge).
            long g0 = System.currentTimeMillis();
            pub.fullyGet(app, key);
            Thread.sleep(2000);
            try {
                Item item = pub.getWithValue(app, key);
                mergedAfterPut = item == null ? "<null-item>" : String.valueOf(item.getValue());
            } catch (Exception e) {
                mergedAfterPut = "<error:" + e.getMessage() + ">";
            }
            tGetMs = System.currentTimeMillis() - g0;
            if (putValue.equals(mergedAfterPut)) {
                steps.add("get_merged_ok");
            } else {
                ok = false;
                errors.add("get_mismatch: expected=" + putValue + " merged=" + mergedAfterPut);
            }

            // Incremental update on top of latest publisher message.
            AbstractMessage base = latest(pub, app, key);
            UUID baseUuid = null;
            if (base != null) baseUuid = base.getUuid();
            if (baseUuid == null) {
                ok = false;
                errors.add("no_base_message_for_incremental");
            } else {
                int want = messageCount(sub, app, key) + 1;
                long i0 = System.currentTimeMillis();
                pub.incrementalUpdate(app, key, baseUuid, Constants.ATOMIC_OPERATION_ADD_ENTRY, "-inc");
                boolean incArrived = waitForMessages(sub, app, key, want, 15000);
                tIncMs = System.currentTimeMillis() - i0;
                subMsgAfterInc = messageCount(sub, app, key);
                Thread.sleep(1000);
                try {
                    Item item = sub.getWithValue(app, key);
                    mergedAfterInc = item == null ? "<null-item>" : String.valueOf(item.getValue());
                } catch (Exception e) {
                    mergedAfterInc = "<error:" + e.getMessage() + ">";
                }
                if (incArrived && (putValue + "-inc").equals(mergedAfterInc)) steps.add("incremental_observed");
                else {
                    ok = false;
                    errors.add("incremental_mismatch: sub messages=" + subMsgAfterInc + " want=" + want
                            + " value=" + mergedAfterInc);
                }
            }

            // Delete then verify merged value clears on subscriber.
            int wantDel = messageCount(sub, app, key) + 1;
            long d0 = System.currentTimeMillis();
            pub.delete(app, key);
            boolean delArrived = waitForMessages(sub, app, key, wantDel, 15000);
            tDelMs = System.currentTimeMillis() - d0;
            subMsgAfterDel = messageCount(sub, app, key);
            Thread.sleep(1000);
            try {
                Item item = sub.getWithValue(app, key);
                mergedAfterDel = item == null ? "<null-item>" : String.valueOf(item.getValue());
            } catch (Exception e) {
                mergedAfterDel = "<error:" + e.getMessage() + ">";
            }
            if (delArrived) steps.add("delete_observed");
            else {
                ok = false;
                errors.add("delete_timeout: sub messages=" + subMsgAfterDel + " want=" + wantDel);
            }
            if (mergedAfterDel == null || "null".equals(mergedAfterDel)) steps.add("delete_merged_ok");
            else {
                ok = false;
                errors.add("delete_mismatch: value=" + mergedAfterDel);
            }
        } catch (Exception e) {
            ok = false;
            errors.add("exception: " + e);
        } finally {
            try { sub.stop(); } catch (Exception ignored) {}
            try { pub.stop(); } catch (Exception ignored) {}
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"baseline\": \"ripple-468e9cb\",\n");
        sb.append("  \"nodes\": \"").append(nodesCsv).append("\",\n");
        sb.append("  \"app\": \"").append(app).append("\", \"key\": \"").append(key).append("\",\n");
        sb.append("  \"ok\": ").append(ok).append(",\n");
        sb.append("  \"steps\": [");
        for (int i = 0; i < steps.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(steps.get(i)).append("\"");
        }
        sb.append("],\n");
        sb.append("  \"errors\": [");
        for (int i = 0; i < errors.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(errors.get(i).replace("\"", "'")).append("\"");
        }
        sb.append("],\n");
        sb.append("  \"put_value\": \"").append(putValue).append("\",\n");
        sb.append("  \"merged_after_put\": \"").append(mergedAfterPut).append("\",\n");
        sb.append("  \"merged_after_inc\": \"").append(mergedAfterInc).append("\",\n");
        sb.append("  \"merged_after_delete\": \"").append(mergedAfterDel).append("\",\n");
        sb.append("  \"publish_to_subscriber_ms\": ").append(tDispatchedMs).append(",\n");
        sb.append("  \"get_roundtrip_ms\": ").append(tGetMs).append(",\n");
        sb.append("  \"incremental_to_subscriber_ms\": ").append(tIncMs).append(",\n");
        sb.append("  \"delete_to_subscriber_ms\": ").append(tDelMs).append(",\n");
        sb.append("  \"sub_msgs_before\": ").append(subMsgBefore).append(",\n");
        sb.append("  \"sub_msgs_after_put\": ").append(subMsgAfterPut).append(",\n");
        sb.append("  \"sub_msgs_after_inc\": ").append(subMsgAfterInc).append(",\n");
        sb.append("  \"sub_msgs_after_delete\": ").append(subMsgAfterDel).append("\n");
        sb.append("}\n");
        Files.write(Paths.get(outJson), sb.toString().getBytes("UTF-8"));
        System.out.println(sb.toString());
        System.exit(ok ? 0 : 1);
    }
}
