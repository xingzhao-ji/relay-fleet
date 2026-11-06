package relay;

import io.grpc.*;
import io.grpc.stub.*;
import relay.proto.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

final class FleetService extends FleetGrpc.FleetImplBase implements AutoCloseable {
    private final Store store;
    private final Semaphore streams=new Semaphore(64);
    private final ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor();
    FleetService(Store store) { this.store=store; }
    private Store.Principal principal() { return IdentityInterceptor.PRINCIPAL.get(); }
    private <T> void unary(StreamObserver<T> response,Supplier<T> operation) {
        try {
            if(Context.current().isCancelled()) throw Status.CANCELLED.asRuntimeException();
            response.onNext(operation.get()); response.onCompleted();
        } catch(StatusRuntimeException e) { response.onError(e); }
        catch(Exception e) { e.printStackTrace(System.err); response.onError(Status.INTERNAL.withDescription("server operation failed").asRuntimeException()); }
    }
    @Override public void register(RegisterRequest r,StreamObserver<Device> o) { unary(o,() -> store.register(principal(),r.getDeviceId(),r.getCertificateSha256())); }
    @Override public void setDesired(DesiredRequest r,StreamObserver<Device> o) { unary(o,() -> store.desire(principal(),r.getDeviceId(),r.getExpectedVersion(),r.getJson())); }
    @Override public void listDevices(ListDevicesRequest r,StreamObserver<Devices> o) { unary(o,() -> store.list(principal(),r.getPageSize(),r.getPageToken())); }
    @Override public void queryTelemetry(QueryRequest r,StreamObserver<Samples> o) { unary(o,() -> store.query(principal(),r.getDeviceId(),r.getAfterSequence(),r.getLimit())); }
    @Override public void revoke(DeviceId r,StreamObserver<Device> o) { unary(o,() -> store.revoke(principal(),r.getDeviceId())); }
    @Override public StreamObserver<DeviceFrame> sync(StreamObserver<ServerFrame> response) {
        var output=(ServerCallStreamObserver<ServerFrame>)response;
        var identity=principal();
        if(!identity.role().equals("device") || !streams.tryAcquire()) {
            response.onError((!identity.role().equals("device") ? Status.PERMISSION_DENIED : Status.RESOURCE_EXHAUSTED).withDescription("device role required and at most 64 active streams").asRuntimeException());
            return new StreamObserver<>() { public void onNext(DeviceFrame v){} public void onError(Throwable t){} public void onCompleted(){} };
        }
        // One inbound frame and one outbound ACK at a time; peer readiness gates demand.
        final class Session implements StreamObserver<DeviceFrame> {
            private final AtomicBoolean ended=new AtomicBoolean();
            private ServerFrame pending;
            private ScheduledFuture<?> expiry;
            synchronized void end(Throwable error) {
                if(!ended.compareAndSet(false,true)) return;
                streams.release(); if(expiry!=null) expiry.cancel(false);
                if(!output.isCancelled()) {
                    if(error==null) output.onCompleted(); else output.onError(error);
                }
            }
            synchronized void flush() {
                if(!ended.get() && pending!=null && output.isReady()) {
                    output.onNext(pending); pending=null; output.request(1);
                }
            }
            public synchronized void onNext(DeviceFrame frame) {
                if(ended.get() || output.isCancelled()) return;
                try {
                    pending=store.ingest(identity,frame);
                    System.out.printf("{\"event\":\"committed\",\"tenant\":\"%s\",\"device\":\"%s\",\"ack\":%d,\"accepted\":%d,\"duplicates\":%d}%n",identity.tenant(),identity.device(),pending.getAckThrough(),pending.getAccepted(),pending.getDuplicates());
                    flush();
                } catch(StatusRuntimeException e) { end(e); }
                catch(Exception e) { e.printStackTrace(System.err); end(Status.INTERNAL.withDescription("sync failed").asRuntimeException()); }
            }
            public void onError(Throwable t) { end(null); }
            public void onCompleted() { end(null); }
        }
        var session=new Session();
        output.disableAutoRequest();
        output.setOnReadyHandler(session::flush);
        output.setOnCancelHandler(() -> session.end(null));
        session.expiry=timer.schedule(() -> session.end(Status.DEADLINE_EXCEEDED.withDescription("renew stream after 15 seconds").asRuntimeException()),15,TimeUnit.SECONDS);
        output.request(1);
        return session;
    }
    @Override public void close() { timer.shutdownNow(); }
}
