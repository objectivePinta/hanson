package net.metrosystems.monitoring;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

@Component
public class HandsonHealthIndicator implements HealthIndicator {

    @Override
    public Health health() {
        return Health.status(Status.UP.getCode()).build();
    }
}
