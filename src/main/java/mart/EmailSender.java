package mart;   

import java.util.Properties;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

public class EmailSender {

    private static final String HOST = "sandbox.smtp.mailtrap.io";
    private static final String USERNAME = System.getenv("MAILTRAP_USER");
    private static final String PASSWORD = System.getenv("MAILTRAP_PASS");

    public static void sendEmail(String toEmail, String couponCode) {
        if (toEmail == null || toEmail.trim().isEmpty()) {
            System.err.println(" Email not sent: recipient email is missing");
            return;
        }

        Properties props = new Properties();
        props.put("mail.smtp.host", HOST);
        props.put("mail.smtp.port", "2525");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");

        Session session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(USERNAME, PASSWORD);
            }
        });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("noreply@mart.com"));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail));
            message.setSubject(" You earned a reward ");

            String body = "Hello,\n\n" +
                    "Congratulations! You just earned a 10% discount coupon.\n\n" +
                    "Coupon Code: " + couponCode + "\n\n" +
                    "Thank you for shopping with us.\n";

            message.setText(body);

            Transport.send(message);
            System.out.println("Email sent successfully to: " + toEmail);

        } catch (MessagingException e) {
            System.err.println("Failed to send email to " + toEmail);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting email test...");

        String testEmail = "testuser@gmail.com";
        sendEmail(testEmail, "CONGRATS10");

        System.out.println("Test finished.");
    }
}