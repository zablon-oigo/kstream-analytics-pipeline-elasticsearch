package sales.dev.rewards;

public class RewardService {

    public static void sendRewardEmail(String email, double totalSpend, double voucherAmount) {
        System.out.printf("Reward email sent to %s! You spent KES %.2f. Voucher: KES %.2f for your next purchase.%n",
                          email, totalSpend, voucherAmount);
    }
}