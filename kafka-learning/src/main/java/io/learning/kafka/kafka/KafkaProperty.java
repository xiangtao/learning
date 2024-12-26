package io.learning.kafka.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class KafkaProperty {

  private String bootstrapServer;
  private String topic;

  private boolean isUseAuth = false;
  private String groupId = "";
  private String autoOffsetReset = "latest";
  private int pollTimeout = 2000;

  private String ack = "all";
  private int retries = 2;

  private String authUser = "bdp_user";
  private String authPwd = "wV8a0Pn8"; // online : DHBeulDD

}
