package mart.streams;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import mart.EmailSender;
import sales.dev.avro.CustomerSpend;
import sales.dev.avro.RewardEvent;

public class RewardTransformer implements FixedKeyProcessor<String, CustomerSpend, RewardEvent> {

    private FixedKeyProcessorContext<String, RewardEvent> context;
    private KeyValueStore<String, Boolean> store;

    @Override
    public void init(FixedKeyProcessorContext<String, RewardEvent> context) {
        this.context = context;
        this.store = context.getStateStore("reward-store");
    }

    @Override
    public void process(FixedKeyRecord<String, CustomerSpend> record) {

        String customerId = record.key();
        CustomerSpend data = record.value();

        if (data == null || data.getTotalSpend() < 5000) return;

        Boolean alreadyRewarded = store.get(customerId);
        if (alreadyRewarded != null && alreadyRewarded) return;

        store.put(customerId, true);

        RewardEvent reward = new RewardEvent();

        reward.put("email", data.getEmail());
        reward.put("customer_id", customerId);
        reward.put("total_spent", data.getTotalSpend());
        reward.put("discount_percentage", 10.0);
        reward.put("coupon_code", "CONGRATS10");
        reward.put("timestamp", System.currentTimeMillis());

        // Send email
        EmailSender.sendEmail(data.getEmail(), "CONGRATS10");

        System.out.println("Reward triggered for: " + customerId +
                " | Email: " + data.getEmail() +
                " | Spend: " + data.getTotalSpend());

        context.forward(record.withValue(reward));
    }

    @Override
    public void close() {}
}