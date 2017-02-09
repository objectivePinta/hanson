package net.metrosystems.data;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.google.common.collect.Lists;

import lombok.Data;

@ConfigurationProperties(prefix = "cassandra")
@Data
public class CassandraProperties {
  private List<String> endpoints = Lists.newArrayList("127.0.0.1");
  private String user = "";
  private String pass = "";
  private boolean authRequired = false;
  private int port = 9042;
  private String replication = "1";
  private String strategyClass = "org.apache.cassandra.locator.SimpleStrategy";
  private String strategyOption = "replication_factor";
  private String writeConsistency = "LOCAL_ONE";
  private String readConsistency = "ONE";
  private String keyspace = "poland_prices_dev";
  private String schemaPath = "classpath:cassandra-schema.xml";
  private boolean createKeyspace = true;
}
