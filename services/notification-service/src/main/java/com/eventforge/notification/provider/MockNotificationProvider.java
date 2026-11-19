package com.eventforge.notification.provider;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import com.eventforge.notification.domain.Notification;

/**
 * Stands in for a real email/SMS provider. Always succeeds; failure simulation arrives with Phase 10.
 */
@Component
@Slf4j
public class MockNotificationProvider {

    public void send(Notification notification) {
        log.info(
                "MockNotificationProvider sending {} notification {} to recipient {}",
                notification.getType(),
                notification.getId(),
                notification.getRecipient());
    }
}
