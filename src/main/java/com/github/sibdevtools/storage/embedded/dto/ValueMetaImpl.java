package com.github.sibdevtools.storage.embedded.dto;

import com.github.sibdevtools.session.api.dto.ValueMeta;
import lombok.*;

import java.time.ZonedDateTime;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValueMetaImpl implements ValueMeta {
    private ZonedDateTime createdAt;
    private ZonedDateTime modifiedAt;
    private ZonedDateTime expiredAt;
    private long version;
}
