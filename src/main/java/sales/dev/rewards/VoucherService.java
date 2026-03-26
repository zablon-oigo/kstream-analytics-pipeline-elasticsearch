package sales.dev.rewards;

import redis.clients.jedis.Jedis;

public class VoucherService {

    private final Jedis jedis;
    private final int ttlSeconds;

    public RedisVoucherService(String redisHost, int redisPort, int ttlSeconds) {
        this.jedis = new Jedis(redisHost, redisPort);
        this.ttlSeconds = ttlSeconds; 
    }


    public String issueVoucher(String customerId) {
        String voucher;
        do {
            voucher = VoucherGenerator.generateVoucher(8); 
        } while (jedis.exists(voucher)); 

        jedis.setex(voucher, ttlSeconds, customerId);

        return voucher;
    }
    
}
