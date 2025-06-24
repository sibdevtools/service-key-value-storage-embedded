package com.github.sibdevtools.storage.embedded.conf;

import com.github.sibdevtools.error.mutable.api.source.ErrorLocalizationsJsonSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@ErrorLocalizationsJsonSource(
        systemCode = "KEY_VALUE_STORAGE_SERVICE",
        iso3Code = "eng",
        path = "classpath:/embedded/key-value-storage/content/errors/eng.json"
)
@ErrorLocalizationsJsonSource(
        systemCode = "KEY_VALUE_STORAGE_SERVICE",
        iso3Code = "rus",
        path = "classpath:/embedded/key-value-storage/content/errors/rus.json"
)
@Configuration
@PropertySource("classpath:/embedded/key-value-storage/application.properties")
@ConditionalOnProperty(name = "service.key-value-storage.mode", havingValue = "EMBEDDED")
public class KeyValueStorageServiceEmbeddedConfig {

}
