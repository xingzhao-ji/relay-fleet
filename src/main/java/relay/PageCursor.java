package relay;

import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Opaque, authenticated keyset cursor. Authorization always still comes from mTLS. */
final class PageCursor {
    private final byte[] key;
    private static final Base64.Encoder ENCODER=Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER=Base64.getUrlDecoder();
    PageCursor(byte[] key) {
        if(key==null || key.length!=32) throw new IllegalStateException("invalid cursor signing key");
        this.key=key.clone();
    }
    private byte[] sign(String prefix) {
        try {
            var mac=Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key,"HmacSHA256"));
            return mac.doFinal(prefix.getBytes(StandardCharsets.US_ASCII));
        } catch(java.security.GeneralSecurityException e) { throw new IllegalStateException(e); }
    }
    String encode(String tenant,String afterDevice) {
        String prefix="v1."+ENCODER.encodeToString((tenant+"\n"+afterDevice).getBytes(StandardCharsets.US_ASCII));
        return prefix+"."+ENCODER.encodeToString(sign(prefix));
    }
    String decode(String token,String tenant) {
        if(token.isEmpty()) return "";
        try {
            if(token.length()>256 || !token.matches("v1\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]{43}")) throw new IllegalArgumentException();
            String[] parts=token.split("\\.",-1);
            byte[] payload=DECODER.decode(parts[1]), signature=DECODER.decode(parts[2]);
            if(!ENCODER.encodeToString(payload).equals(parts[1]) || !ENCODER.encodeToString(signature).equals(parts[2])
                || !MessageDigest.isEqual(sign(parts[0]+"."+parts[1]),signature)) throw new IllegalArgumentException();
            String[] fields=new String(payload,StandardCharsets.UTF_8).split("\n",-1);
            if(fields.length!=2 || !fields[0].matches("[a-zA-Z0-9_-]{1,64}")
                || !fields[1].matches("[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}") || !fields[0].equals(tenant)) throw new IllegalArgumentException();
            return fields[1];
        } catch(IllegalArgumentException e) {
            throw Status.INVALID_ARGUMENT.withDescription("invalid page_token for this tenant").asRuntimeException();
        }
    }
}
