package org.gojo.learn.scheduler.sdk.config.datetime;



import java.util.TimeZone;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DateConfig {

  public static final String INDIAN_TIME_ZONE_ZONE_ID = "Asia/Kolkata";

  public DateConfig() {
    TimeZone.setDefault(TimeZone.getTimeZone(INDIAN_TIME_ZONE_ZONE_ID));
  }

}
