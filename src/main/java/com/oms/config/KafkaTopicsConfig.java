package com.oms.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.topics")
public class KafkaTopicsConfig {
  public String inventoryCheckRequest;
  public String inventoryCheckResponse;
  public String orderEvents;
}
