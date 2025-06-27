package org.gojo.learn.scheduler.sdk.config.id.generator;

import java.security.SecureRandom;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class TriggerIdGenerator {

  private static final int RANDOM_BOUND_UPPER = 9000; // Max value for the random number
  private static final int RANDOM_BOUND_LOWER = 1000; // Min value for the random number
  private static final int RANDOM_DIGITS = 4; // Number of digits for the random number
  private SecureRandom secureRandom;

  // Singleton is the default scope of all beans defined in Spring
  @Bean
  public SecureRandom initTriggerIdGenerator() {
    this.secureRandom = new SecureRandom();
    return secureRandom;
  }

  public String generateId(String prefix) {
    long currentTime = System.currentTimeMillis();
    int secureNumber = generateSecureNumber();
    return prefix + currentTime + secureNumber;
  }

  private int generateSecureNumber() {
    // Generate a random number within the specified bounds
    return secureRandom.nextInt(RANDOM_BOUND_LOWER, RANDOM_BOUND_UPPER);
  }

  public String generateUniqueId() {
    return generateId("TRIG");
  }

}
