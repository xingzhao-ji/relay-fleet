package relay;

import io.grpc.*;
import javax.net.ssl.SSLSession;

/** Identity is the registered leaf-certificate SHA-256, never user metadata/CN. */
final class IdentityInterceptor implements ServerInterceptor {
    static final Context.Key<Store.Principal> PRINCIPAL=Context.key("certificate-principal");
    private final Store store;
    IdentityInterceptor(Store store) { this.store=store; }
    public <Q,S> ServerCall.Listener<Q> interceptCall(ServerCall<Q,S> call, Metadata headers, ServerCallHandler<Q,S> next) {
        try {
            SSLSession tls=call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            Store.require(tls!=null,Status.UNAUTHENTICATED,"mTLS required");
            var p=store.principal(Store.sha(tls.getPeerCertificates()[0].getEncoded()));
            return Contexts.interceptCall(Context.current().withValue(PRINCIPAL,p),call,headers,next);
        } catch(Exception e) {
            Status status=e instanceof StatusRuntimeException s ? s.getStatus() : Status.UNAUTHENTICATED.withDescription("client certificate required");
            System.out.printf("{\"event\":\"auth_rejected\",\"status\":\"%s\"}%n",status.getCode());
            call.close(status,new Metadata()); return new ServerCall.Listener<>() {};
        }
    }
}
