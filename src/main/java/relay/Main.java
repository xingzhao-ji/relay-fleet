package relay;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

public final class Main {
    public static void main(String[] args) throws Exception {
        if(args.length==4 && args[0].equals("bootstrap")) {
            try(var store=new Store(args[1])) { store.bootstrapOperator(args[2],args[3]); }
            System.out.println("{\"event\":\"operator_provisioned\"}"); return;
        }
        if(args.length!=6 || !args[0].equals("serve")) {
            System.err.println("bootstrap DB CERT_SHA256 TENANT | serve DB PORT SERVER_CERT SERVER_KEY CA_CERT"); System.exit(2);
        }
        var store=new Store(args[1]); var service=new FleetService(store);
        var executor=new ThreadPoolExecutor(8,8,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(128),new ThreadPoolExecutor.CallerRunsPolicy());
        var ssl=GrpcSslContexts.forServer(new File(args[3]),new File(args[4])).trustManager(new File(args[5])).clientAuth(ClientAuth.REQUIRE).build();
        var server=NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1",Integer.parseInt(args[2])))
            .sslContext(ssl).executor(executor).maxInboundMessageSize(80*1024).maxConcurrentCallsPerConnection(32)
            .maxConnectionIdle(30,TimeUnit.SECONDS).maxConnectionAge(120,TimeUnit.SECONDS).maxConnectionAgeGrace(5,TimeUnit.SECONDS)
            .intercept(new IdentityInterceptor(store)).addService(service).build().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try { if(!server.awaitTermination(3,TimeUnit.SECONDS)) server.shutdownNow(); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
            service.close();executor.shutdownNow();
            try { store.close(); } catch(Exception e) { e.printStackTrace(System.err); }
        }));
        System.out.printf("{\"event\":\"ready\",\"port\":%d,\"transport\":\"mTLS\",\"simulation\":true}%n",server.getPort());
        server.awaitTermination();
    }
}
