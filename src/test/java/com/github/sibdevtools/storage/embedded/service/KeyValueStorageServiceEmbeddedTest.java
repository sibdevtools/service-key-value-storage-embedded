package com.github.sibdevtools.storage.embedded.service;

import com.github.sibdevtools.session.api.rq.SetValueRq;
import com.github.sibdevtools.storage.embedded.exception.UnexpectedErrorException;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueStorageServiceEmbeddedTest {

    private KeyValueStorageServiceEmbedded service;

    @BeforeEach
    void setUp() {
        service = new KeyValueStorageServiceEmbedded();
    }

    @Test
    void testSetAndGetValue() {
        val space = UUID.randomUUID().toString();
        val key = UUID.randomUUID().toString();
        val value = UUID.randomUUID().toString();
        val expiredAt = ZonedDateTime.now()
                .plusMinutes(1);

        val rq = SetValueRq.builder()
                .space(space)
                .key(key)
                .value(value)
                .expiredAt(expiredAt)
                .build();

        val setRs = service.set(rq);
        assertNotNull(setRs);
        assertEquals(expiredAt, setRs.getBody().getExpiredAt());

        val getOpt = service.get(space, key);
        assertTrue(getOpt.isPresent());
        val getRs = getOpt.get();
        assertEquals(value, getRs.getBody().getValue());
    }

    @Test
    void testGetKeysNonExistedSpace() {
        val space = UUID.randomUUID().toString();
        val keys = service.getKeys(space);
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    void testGetKeysExpiredKey() {
        val space = UUID.randomUUID().toString();
        service.set(
                SetValueRq.builder()
                        .space(space)
                        .key(UUID.randomUUID().toString())
                        .expiredAt(ZonedDateTime.now().minusMinutes(1))
                        .build()
        );

        val keys = service.getKeys(space);
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    void testGetSpacesAndKeys() {
        val space1 = UUID.randomUUID().toString();
        val keyA = UUID.randomUUID().toString();
        service.set(SetValueRq.builder()
                .space(space1)
                .key(keyA)
                .value("valueA")
                .expiredAt(ZonedDateTime.now().plusMinutes(5))
                .build());

        val space2 = UUID.randomUUID().toString();
        service.set(SetValueRq.builder()
                .space(space2)
                .key("keyB")
                .value("valueB")
                .expiredAt(ZonedDateTime.now().plusMinutes(5))
                .build());

        val spaces = service.getSpaces();
        assertEquals(Set.of(space1, space2), spaces);

        val keys = service.getKeys(space1);
        assertEquals(Set.of(keyA), keys);
    }

    @Test
    void testDeleteKeyAndSpace() {
        val space = UUID.randomUUID().toString();
        val key = UUID.randomUUID().toString();

        service.set(SetValueRq.builder()
                .space(space)
                .key(key)
                .value("value")
                .expiredAt(ZonedDateTime.now().plusMinutes(1))
                .build());

        service.delete(space, key);
        assertTrue(service.get(space, key).isEmpty());

        service.set(SetValueRq.builder()
                .space(space)
                .key(key)
                .value("value")
                .expiredAt(ZonedDateTime.now().plusMinutes(1))
                .build());

        service.delete(space);
        assertTrue(service.get(space, key).isEmpty());
        assertFalse(service.getSpaces().contains(space));
    }

    @Test
    void testProlongate() {
        val space = UUID.randomUUID().toString();
        val key = UUID.randomUUID().toString();
        val initialExpire = ZonedDateTime.now().plusMinutes(1);
        val newExpire = ZonedDateTime.now().plusMinutes(5);

        service.set(SetValueRq.builder()
                .space(space)
                .key(key)
                .value("val")
                .expiredAt(initialExpire)
                .build());

        val prolongated = service.prolongate(space, key, newExpire);
        assertEquals(newExpire, prolongated.getBody().getExpiredAt());

        val result = service.get(space, key);
        assertTrue(result.isPresent());
        assertEquals("val", result.get().getBody().getValue());
    }

    @Test
    void testProlongateOnNonExistentKey() {
        val exception = assertThrows(UnexpectedErrorException.class, () -> {
            service.prolongate("unknown", "key", ZonedDateTime.now().plusMinutes(1));
        });
        assertEquals("Space not found", exception.getMessage());
    }

    @Test
    void testProlongateOnExpiredKey() {
        val space = UUID.randomUUID().toString();
        val key = UUID.randomUUID().toString();
        val expiredAt = ZonedDateTime.now().minusSeconds(1);

        service.set(SetValueRq.builder()
                .space(space)
                .key(key)
                .value("will expire")
                .expiredAt(expiredAt)
                .build());

        val exception = assertThrows(UnexpectedErrorException.class, () -> {
            service.prolongate(space, key, ZonedDateTime.now().plusSeconds(5));
        });

        assertEquals("Key not found or expired", exception.getMessage());
    }

    @Test
    void testExpiredValueIsRemoved() throws InterruptedException {
        val space = UUID.randomUUID().toString();
        val key = UUID.randomUUID().toString();

        service.set(SetValueRq.builder()
                .space(space)
                .key(key)
                .value("expiring")
                .expiredAt(ZonedDateTime.now().plusSeconds(1))
                .build());

        Thread.sleep(1500);

        assertTrue(service.get(space, key).isEmpty());
        assertFalse(service.getKeys(space).contains(key));
    }
}
