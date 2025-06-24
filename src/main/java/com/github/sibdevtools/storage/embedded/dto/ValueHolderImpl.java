package com.github.sibdevtools.storage.embedded.dto;

import com.github.sibdevtools.session.api.dto.ValueHolder;
import lombok.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValueHolderImpl implements ValueHolder {
    private Object value;
    private ValueMetaImpl meta;
}
