package relay;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.*;
import relay.proto.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import static org.junit.jupiter.api.Assertions.*;

class StoreTest {
    @TempDir Path dir;
    Store store;
    Store.Principal op = new Store.Principal("aa", "operator", "north", "");
    Store.Principal dev = new Store.Principal("b".repeat(64), "device", "north", "sim-1");
    @BeforeEach void setup() throws Exception {
        store = new Store(dir.resolve("fleet.db").toString());
        store.register(op, "sim-1", "b".repeat(64));
    }
    @AfterEach void close() throws Exception { store.close(); }
    DeviceFrame frame(long sequence, String body) {
        return DeviceFrame.newBuilder().setDeviceId("sim-1").setSchemaVersion(1)
            .addSamples(Sample.newBuilder().setSequence(sequence).setObservedAtMs(1000).setReadingJson(body)).build();
    }
    void status(Status.Code code, Runnable fn) { assertEquals(code, assertThrows(StatusRuntimeException.class, fn::run).getStatus().getCode()); }
    @Test void replayHasOneEffectAndSurvivesReopen() throws Exception {
        assertEquals(1, store.ingest(dev, frame(1,"{}" )).getAccepted());
        store.close(); store = new Store(dir.resolve("fleet.db").toString());
        assertEquals(1, store.ingest(dev, frame(1,"{}" )).getDuplicates());
        assertEquals(1, store.get(op, "sim-1").getTelemetryCount());
    }
    @Test void conflictingSequenceAndGapHaveNoEffect() {
        store.ingest(dev, frame(1,"{}"));
        status(Status.Code.ALREADY_EXISTS, () -> store.ingest(dev, frame(1,"different")));
        status(Status.Code.FAILED_PRECONDITION, () -> store.ingest(dev, frame(3,"{}")));
        assertEquals(1,store.get(op,"sim-1").getLastSequence());
    }
    @Test void batchRollsBackBeforeAckWhenLaterSampleConflicts() {
        var bad = frame(1,"{}").toBuilder().addSamples(Sample.newBuilder().setSequence(3).setReadingJson("{}" )).build();
        status(Status.Code.FAILED_PRECONDITION, () -> store.ingest(dev,bad));
        assertEquals(0,store.get(op,"sim-1").getTelemetryCount());
    }
    @Test void certificateBoundIdentityRejectsSpoofAndTenantCannotQuery() {
        status(Status.Code.PERMISSION_DENIED, () -> store.ingest(dev,frame(1,"{}").toBuilder().setDeviceId("sim-2").build()));
        var other = new Store.Principal("cc","operator","south","");
        status(Status.Code.NOT_FOUND, () -> store.get(other,"sim-1"));
        status(Status.Code.PERMISSION_DENIED, () -> store.register(dev,"sim-2","d".repeat(64)));
    }
    @Test void configCompareAndSwapAndAcknowledgmentNeverRegress() {
        var one = store.desire(op,"sim-1",0,"{\"sample_interval_ms\":100}");
        var two = store.desire(op,"sim-1",1,"{\"sample_interval_ms\":200}");
        status(Status.Code.ABORTED, () -> store.desire(op,"sim-1",1,"{}"));
        store.ingest(dev,DeviceFrame.newBuilder().setDeviceId("sim-1").setSchemaVersion(1).setAppliedVersion(2).setAppliedSha256(two.getDesired().getSha256()).build());
        store.ingest(dev,DeviceFrame.newBuilder().setDeviceId("sim-1").setSchemaVersion(1).setAppliedVersion(1).setAppliedSha256(one.getDesired().getSha256()).build());
        assertEquals(2,store.get(op,"sim-1").getAppliedVersion());
        status(Status.Code.FAILED_PRECONDITION, () -> store.ingest(dev,DeviceFrame.newBuilder().setDeviceId("sim-1").setSchemaVersion(1).setAppliedVersion(2).setAppliedSha256("wrong").build()));
    }
    @Test void concurrentRetriesRemainUnique() throws Exception {
        try(var pool = Executors.newFixedThreadPool(8)) {
            var tasks = new java.util.ArrayList<Callable<ServerFrame>>();
            for(int i=0;i<32;i++) tasks.add(() -> store.ingest(dev,frame(1,"{}")));
            int accepted=0; for(var f:pool.invokeAll(tasks)) accepted+=f.get().getAccepted();
            assertEquals(1,accepted);
        }
        assertEquals(1,store.get(op,"sim-1").getTelemetryCount());
    }
    @Test void revokedDeviceCannotContinueExistingSession() {
        store.revoke(op,"sim-1");
        status(Status.Code.PERMISSION_DENIED, () -> store.ingest(dev,frame(1,"{}")));
    }
    @Test void oversizeAndUnsupportedProtocolRejectedBeforeWrite() {
        status(Status.Code.INVALID_ARGUMENT, () -> store.ingest(dev,frame(1,"x".repeat(2049))));
        status(Status.Code.FAILED_PRECONDITION, () -> store.ingest(dev,frame(1,"{}").toBuilder().setSchemaVersion(2).build()));
        assertEquals(0,store.get(op,"sim-1").getTelemetryCount());
    }
    @Test void unusableConfigCannotBecomeDesiredState() {
        for(String bad:new String[]{"{}","[]","not-json","{\"sample_interval_ms\":true}","{\"sample_interval_ms\":1}","{\"sample_interval_ms\":10.5}","{\"sample_interval_ms\":\"100\"}","{\"sample_interval_ms\":18446744073709551626}"})
            status(Status.Code.INVALID_ARGUMENT, () -> store.desire(op,"sim-1",0,bad));
        assertEquals(0,store.get(op,"sim-1").getDesired().getVersion());
    }
    @Test void configurationMustBeStrictJsonForPythonInteroperability() {
        for(String bad:new String[]{"{sample_interval_ms:100}","{'sample_interval_ms':100}","{\"sample_interval_ms\":100}//comment"})
            status(Status.Code.INVALID_ARGUMENT, () -> store.desire(op,"sim-1",0,bad));
        assertEquals(0,store.get(op,"sim-1").getDesired().getVersion());
    }
}
