package net.metrosystems;

import net.metrosystems.data.CassandraProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({ CassandraProperties.class})
public class HandsonApplication {

	public static void main(String[] args) {
		SpringApplication.run(HandsonApplication.class, args);
	}
}
