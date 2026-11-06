package relay;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import relay.proto.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.*;
import java.util.HexFormat;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.Strictness;

/** Single-process, serialized SQLite transaction boundary. ACK follows FULL commit. */
public final class Store implements AutoCloseable {
    public record Principal(String fingerprint, String role, String tenant, String device) {}
    private final Connection db;
    private final PageCursor cursors;
    public Store(String path) throws SQLException {
        db = DriverManager.getConnection("jdbc:sqlite:"+path);
        try(var s=db.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL"); s.execute("PRAGMA synchronous=FULL");
            s.execute("PRAGMA foreign_keys=ON"); s.execute("PRAGMA busy_timeout=5000");
            s.execute("CREATE TABLE IF NOT EXISTS devices(tenant TEXT,device TEXT,last_seq INTEGER NOT NULL DEFAULT 0,desired INTEGER NOT NULL DEFAULT 0,applied INTEGER NOT NULL DEFAULT 0,last_seen INTEGER NOT NULL DEFAULT 0,clock_skew INTEGER NOT NULL DEFAULT 0,revoked INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(tenant,device))");
            s.execute("CREATE TABLE IF NOT EXISTS principals(fingerprint TEXT PRIMARY KEY,role TEXT NOT NULL,tenant TEXT NOT NULL,device TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1)");
            s.execute("CREATE TABLE IF NOT EXISTS configs(tenant TEXT,device TEXT,version INTEGER,json TEXT NOT NULL,sha TEXT NOT NULL,PRIMARY KEY(tenant,device,version),FOREIGN KEY(tenant,device) REFERENCES devices(tenant,device))");
            s.execute("CREATE TABLE IF NOT EXISTS telemetry(tenant TEXT,device TEXT,seq INTEGER,observed INTEGER NOT NULL,body TEXT NOT NULL,sha TEXT NOT NULL,received INTEGER NOT NULL,PRIMARY KEY(tenant,device,seq),FOREIGN KEY(tenant,device) REFERENCES devices(tenant,device))");
            s.execute("CREATE TABLE IF NOT EXISTS service_settings(name TEXT PRIMARY KEY,value BLOB NOT NULL)");
        }
        byte[] key=new byte[32];new SecureRandom().nextBytes(key);
        update("INSERT OR IGNORE INTO service_settings(name,value) VALUES('list_cursor_key',?)",key);
        try(var q=sql("SELECT value FROM service_settings WHERE name='list_cursor_key'");var r=q.executeQuery()) {
            if(!r.next()) throw new SQLException("cursor signing key missing");
            cursors=new PageCursor(r.getBytes(1));
        }
    }
    public static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch(Exception e) { throw new IllegalStateException(e); }
    }
    static void require(boolean value, Status status, String message) { if(!value) throw status.withDescription(message).asRuntimeException(); }
    private void operator(Principal p) { require(p.role().equals("operator"),Status.PERMISSION_DENIED,"operator certificate required"); }
    private PreparedStatement sql(String query,Object...args) throws SQLException {
        var p=db.prepareStatement(query); for(int i=0;i<args.length;i++) p.setObject(i+1,args[i]); return p;
    }
    private int update(String query,Object...args) throws SQLException { try(var p=sql(query,args)) { return p.executeUpdate(); } }
    private StatusRuntimeException failure(SQLException e) { return Status.INTERNAL.withDescription("database operation failed").withCause(e).asRuntimeException(); }
    public synchronized void bootstrapOperator(String fingerprint,String tenant) throws SQLException {
        require(fingerprint.matches("[a-f0-9]{64}") && tenant.matches("[a-zA-Z0-9_-]{1,64}"),Status.INVALID_ARGUMENT,"invalid principal");
        update("INSERT INTO principals(fingerprint,role,tenant,device) VALUES(?,'operator',?,'')",fingerprint,tenant);
    }
    public synchronized Principal principal(String fingerprint) {
        try(var q=sql("SELECT * FROM principals WHERE fingerprint=? AND active=1",fingerprint);var r=q.executeQuery()) {
            require(r.next(),Status.UNAUTHENTICATED,"certificate is not registered or is revoked");
            return new Principal(fingerprint,r.getString("role"),r.getString("tenant"),r.getString("device"));
        } catch(SQLException e) { throw failure(e); }
    }
    public synchronized Device register(Principal p,String id,String fingerprint) {
        operator(p);
        require(id.matches("[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}") && fingerprint.matches("[a-f0-9]{64}"),Status.INVALID_ARGUMENT,"invalid device ID or certificate fingerprint");
        try {
            db.setAutoCommit(false);
            update("INSERT INTO devices(tenant,device) VALUES(?,?)",p.tenant(),id);
            update("INSERT INTO principals(fingerprint,role,tenant,device) VALUES(?,'device',?,?)",fingerprint,p.tenant(),id);
            db.commit();
            return fetch(p.tenant(),id);
        } catch(SQLException e) { rollback(); throw Status.ALREADY_EXISTS.withDescription("device or certificate already registered").asRuntimeException(); }
        finally { autoCommit(); }
    }
    private Config config(String tenant,String id,long version) throws SQLException {
        if(version==0) return Config.getDefaultInstance();
        try(var q=sql("SELECT json,sha FROM configs WHERE tenant=? AND device=? AND version=?",tenant,id,version);var r=q.executeQuery()) {
            require(r.next(),Status.FAILED_PRECONDITION,"unknown config version");
            return Config.newBuilder().setVersion(version).setJson(r.getString(1)).setSha256(r.getString(2)).build();
        }
    }
    private Device fetch(String tenant,String id) throws SQLException {
        try(var q=sql("SELECT d.*,(SELECT count(*) FROM telemetry t WHERE t.tenant=d.tenant AND t.device=d.device) AS n FROM devices d WHERE tenant=? AND device=?",tenant,id);var r=q.executeQuery()) {
            require(r.next(),Status.NOT_FOUND,"device not found in certificate tenant");
            return Device.newBuilder().setTenant(tenant).setDeviceId(id).setLastSequence(r.getLong("last_seq"))
                .setDesired(config(tenant,id,r.getLong("desired"))).setAppliedVersion(r.getLong("applied"))
                .setLastSeenMs(r.getLong("last_seen")).setClockSkewMs(r.getLong("clock_skew"))
                .setTelemetryCount(r.getLong("n")).setRevoked(r.getBoolean("revoked")).build();
        }
    }
    public synchronized Device get(Principal p,String id) { operator(p); try { return fetch(p.tenant(),id); } catch(SQLException e) { throw failure(e); } }
    public synchronized Devices list(Principal p,int pageSize,String pageToken) {
        operator(p);
        require(pageSize>=1 && pageSize<=100,Status.INVALID_ARGUMENT,"page_size must be 1..100; regenerate legacy listing clients");
        String after=cursors.decode(pageToken,p.tenant());
        var out=Devices.newBuilder();
        // IDs are immutable, ASCII and BINARY-ordered. Read one extra ID to signal
        // continuation, but fetch at most pageSize full configurations.
        try(var q=sql("SELECT device FROM devices WHERE tenant=? AND device>? ORDER BY device COLLATE BINARY LIMIT ?",p.tenant(),after,pageSize+1);var r=q.executeQuery()) {
            String last="";
            while(r.next()) {
                if(out.getDevicesCount()==pageSize) {
                    out.setNextPageToken(cursors.encode(p.tenant(),last));break;
                }
                last=r.getString(1);out.addDevices(fetch(p.tenant(),last));
            }
            var result=out.build();
            require(result.getSerializedSize()<=512*1024,Status.RESOURCE_EXHAUSTED,"listing page exceeded contract size bound");
            return result;
        } catch(SQLException e) { throw failure(e); }
    }
    public synchronized Device desire(Principal p,String id,long expected,String body) {
        operator(p); require(body.getBytes(StandardCharsets.UTF_8).length<=4096 && !body.isBlank(),Status.INVALID_ARGUMENT,"config size must be 1..4096 bytes");
        try {
            db.setAutoCommit(false); var old=fetch(p.tenant(),id);
            require(old.getDesired().getVersion()==expected,Status.ABORTED,"desired version changed; reload before retry");
            try {
                var value=new GsonBuilder().setStrictness(Strictness.STRICT).create().fromJson(body,JsonObject.class).get("sample_interval_ms");
                require(value!=null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber()
                    && value.getAsString().matches("[0-9]+") && value.getAsBigDecimal().longValueExact()>=10 && value.getAsBigDecimal().longValueExact()<=60000,
                    Status.INVALID_ARGUMENT,"sample_interval_ms must be integer 10..60000");
            } catch(StatusRuntimeException e) { throw e; }
            catch(Exception e) { throw Status.INVALID_ARGUMENT.withDescription("invalid configuration JSON").asRuntimeException(); }
            require(expected>=0 && expected<Long.MAX_VALUE,Status.INVALID_ARGUMENT,"version overflow");
            long next=expected+1;
            update("INSERT INTO configs VALUES(?,?,?,?,?)",p.tenant(),id,next,body,sha(body.getBytes(StandardCharsets.UTF_8)));
            update("UPDATE devices SET desired=? WHERE tenant=? AND device=?",next,p.tenant(),id);
            db.commit(); return fetch(p.tenant(),id);
        } catch(SQLException e) { rollback(); throw failure(e); }
        catch(RuntimeException e) { rollback(); throw e; }
        finally { autoCommit(); }
    }
    public synchronized Device revoke(Principal p,String id) {
        operator(p);
        try {
            db.setAutoCommit(false); fetch(p.tenant(),id);
            update("UPDATE principals SET active=0 WHERE tenant=? AND device=? AND role='device'",p.tenant(),id);
            update("UPDATE devices SET revoked=1 WHERE tenant=? AND device=?",p.tenant(),id);
            db.commit(); return fetch(p.tenant(),id);
        } catch(SQLException e) { rollback(); throw failure(e); }
        catch(RuntimeException e) { rollback(); throw e; }
        finally { autoCommit(); }
    }
    public synchronized Samples query(Principal p,String id,long after,int limit) {
        operator(p); require(after>=0 && limit>=1 && limit<=256,Status.INVALID_ARGUMENT,"query limit must be 1..256");
        try { fetch(p.tenant(),id);
            var out=Samples.newBuilder();
            try(var q=sql("SELECT seq,observed,body FROM telemetry WHERE tenant=? AND device=? AND seq>? ORDER BY seq LIMIT ?",p.tenant(),id,after,limit);var r=q.executeQuery()) {
                while(r.next()) out.addSamples(Sample.newBuilder().setSequence(r.getLong(1)).setObservedAtMs(r.getLong(2)).setReadingJson(r.getString(3)));
            } return out.build();
        } catch(SQLException e) { throw failure(e); }
    }
    public synchronized ServerFrame ingest(Principal p,DeviceFrame frame) {
        require(p.role().equals("device") && p.device().equals(frame.getDeviceId()),Status.PERMISSION_DENIED,"device certificate does not match claimed device");
        require(frame.getSchemaVersion()==1,Status.FAILED_PRECONDITION,"supported schema_version is 1");
        require(frame.getSamplesCount()<=32,Status.RESOURCE_EXHAUSTED,"at most 32 samples per frame");
        try {
            db.setAutoCommit(false);
            var old=fetch(p.tenant(),p.device());
            require(!old.getRevoked(),Status.PERMISSION_DENIED,"device revoked");
            require(principal(p.fingerprint()).equals(p),Status.PERMISSION_DENIED,"principal binding changed");
            long last=old.getLastSequence(), now=System.currentTimeMillis(), skew=old.getClockSkewMs();
            int accepted=0,duplicates=0;
            for(var sample:frame.getSamplesList()) {
                require(sample.getSequence()>0 && sample.getReadingJson().getBytes(StandardCharsets.UTF_8).length<=2048,Status.INVALID_ARGUMENT,"invalid sequence or sample exceeds 2048 bytes");
                String hash=sha(sample.toByteArray());
                if(sample.getSequence()<=last) {
                    try(var q=sql("SELECT sha FROM telemetry WHERE tenant=? AND device=? AND seq=?",p.tenant(),p.device(),sample.getSequence());var r=q.executeQuery()) {
                        require(r.next() && r.getString(1).equals(hash),Status.ALREADY_EXISTS,"sequence replay has conflicting content");
                    } duplicates++;
                } else {
                    require(sample.getSequence()==last+1,Status.FAILED_PRECONDITION,"sequence gap; replay from last ACK");
                    update("INSERT INTO telemetry VALUES(?,?,?,?,?,?,?)",p.tenant(),p.device(),sample.getSequence(),sample.getObservedAtMs(),sample.getReadingJson(),hash,now);
                    last=sample.getSequence(); accepted++;
                    // Saturating subtract avoids diagnostic overflow from hostile device clocks.
                    try { skew=Math.subtractExact(sample.getObservedAtMs(),now); } catch(ArithmeticException e) { skew=Long.MIN_VALUE; }
                }
            }
            long applied=old.getAppliedVersion();
            require(frame.getAppliedVersion()>=0,Status.INVALID_ARGUMENT,"invalid applied version");
            if(frame.getAppliedVersion()>0) {
                var c=config(p.tenant(),p.device(),frame.getAppliedVersion());
                require(c.getSha256().equals(frame.getAppliedSha256()),Status.FAILED_PRECONDITION,"applied config hash mismatch");
                applied=Math.max(applied,frame.getAppliedVersion());
            }
            update("UPDATE devices SET last_seq=?,applied=?,last_seen=?,clock_skew=? WHERE tenant=? AND device=?",last,applied,now,skew,p.tenant(),p.device());
            var response=ServerFrame.newBuilder().setAckThrough(last).setAccepted(accepted).setDuplicates(duplicates)
                .setDesired(old.getDesired()).setServerTimeMs(now).build();
            db.commit(); return response;
        } catch(SQLException e) { rollback(); throw failure(e); }
        catch(RuntimeException e) { rollback(); throw e; }
        finally { autoCommit(); }
    }
    private void rollback() { try { db.rollback(); } catch(SQLException e) { throw failure(e); } }
    private void autoCommit() { try { db.setAutoCommit(true); } catch(SQLException e) { throw failure(e); } }
    @Override public synchronized void close() throws SQLException { db.close(); }
}
