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
    /**
     * Date-time of creation
     */
    private ZonedDateTime createdAt;
    /**
     * Date-time of last modification
     */
    private ZonedDateTime modifiedAt;
    /**
     * Optional
     * Date-time of version expiration
     */
    private ZonedDateTime expiredAt;
    /**
     * Modification version
     */
    private long version;
}
