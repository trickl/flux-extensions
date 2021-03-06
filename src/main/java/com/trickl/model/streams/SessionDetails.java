package com.trickl.model.streams;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Builder
@Data
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SessionDetails {
  @Nonnull protected String id;

  protected String userName;
  protected Instant connectionTime;
  protected Instant disconnectionTime;
}
