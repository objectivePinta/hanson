package net.metrosystems.data;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.mapping.MappingManager;


@Service
public class CassandraPersistenceService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPersistenceService.class);
  private final CassandraProperties properties;
  private final Resource dbSchemaResource;
  private MappingManager mappingManager;
  private Session session;
  private ConsistencyLevel WRITE_CONSISTENCY_LEVEL;
  private ConsistencyLevel READ_CONSISTENCY_LEVEL;
  private Cluster cluster;

  public CassandraPersistenceService(ResourceLoader resourceLoader, CassandraProperties properties) {
    this.properties = properties;
    if (properties.getSchemaPath() == null) {
      this.dbSchemaResource = resourceLoader.getResource("classpath:cassandra-schema.xml");
    }
    else {
      this.dbSchemaResource = resourceLoader.getResource(properties.getSchemaPath());
    }
  }

  @PostConstruct
  void connectDatabase() {
    try {
      WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(properties.getWriteConsistency());
      READ_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(properties.getReadConsistency());
      Builder clusterBuilder = Cluster.builder()
          .addContactPoints(properties.getEndpoints().toArray(new String[properties.getEndpoints().size()]))
          .withPort(properties.getPort()).withCompression(ProtocolOptions.Compression.SNAPPY);
      if (properties.isAuthRequired()) {
        clusterBuilder.withCredentials(properties.getUser(), properties.getPass());
      }
      cluster = clusterBuilder.build();
      this.registerCodecs(cluster);
      session = cluster.connect();
      mappingManager = new MappingManager(session);
      initDatabase();
    }
    catch (Exception e) {
      LOGGER.error("Could not connect to the database cluster {}", e.getMessage());
    }

  }

  void registerCodecs(Cluster cluster) {
    cluster.getConfiguration().getCodecRegistry().register(TypeCodec.list(TypeCodec.uuid()));
  }

  @PreDestroy
  void disconnectDatabase() {
    try {
      cluster.close();
    }
    catch (Exception e) {
      LOGGER.error("Could not close database exception {}", e.getMessage());
    }
  }

  public boolean checkConnection() {
    return !session.isClosed();
  }

  public MappingManager getMappingManager() {
    return mappingManager;
  }

  private void initDatabase() {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    Document document;
    try {
      DocumentBuilder builder = factory.newDocumentBuilder();
      document = builder.parse(dbSchemaResource.getInputStream());
    }
    catch (ParserConfigurationException | SAXException | IOException e) {
      LOGGER.error("Could not initialize database", e);
      return;
    }

    // it need to create schema with specific strategy_class and strategy_options every enviroment has his properties
    String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS " + properties.getKeyspace()
        + " WITH replication = {'class': '" + properties.getStrategyClass() + "', '" + properties.getStrategyOption()
        + "': '" + properties.getReplication() + "'}  AND durable_writes = true";

    try {
      if (properties.isCreateKeyspace()) {
        execute(createKeyspace);
      }
      execute("USE " + properties.getKeyspace() + ";");
    }
    catch (Exception e) {
      LOGGER.error("Could not create keyspace " + properties.getKeyspace(), e);
    }

    DbSchemaTags[] xmlTagsToBeProcessed = DbSchemaTags.values();
    for (DbSchemaTags tag : xmlTagsToBeProcessed) {
      NodeList statementListBasedOnTagName = document.getElementsByTagName(tag.getTagName());
      for (int i = 0; i < statementListBasedOnTagName.getLength(); i++) {
        if (!statementListBasedOnTagName.item(i).getTextContent().trim().isEmpty()) {
          String statementToBeExecuted = statementListBasedOnTagName.item(i).getTextContent().replaceAll("\\s+", " ");
          try {
            execute(statementToBeExecuted);
          }
          catch (Exception e) {
            LOGGER.error("Could not run query " + statementToBeExecuted, e);
          }
        }
      }
    }
  }

  /**
   * Execute the given select query.
   *
   * @param query
   * @return result of the select query
   */
  public ResultSet executeSelect(String query) {
    return session.execute(query);
  }

  /**
   * Execute the given select query with the given values.
   *
   * @param query
   * @param values
   * @return result of the select query
   */
  public ResultSet executeSelect(String query, Object... values) {
    return session.execute(query, values);
  }

  /**
   * Execute a PreparedStatement holding a select query with the given binding values.
   *
   * @param preparedStatement
   * @param values
   * @return result of the select query
   */
  public ResultSet executeSelect(PreparedStatement preparedStatement, Object... values) {
    return session.execute(preparedStatement.bind(values));
  }

  /**
   * Execute the given update/insert statement (ie: no select queries).
   *
   * @param statement
   */
  public void execute(Statement statement) {
    session.execute(statement);
  }

  /**
   * Execute the given update/insert query (ie: no select queries).
   *
   * @param query
   */
  public void execute(String query) {
    LOGGER.debug("query:" + query);
    query = StringUtils.trimToEmpty(query);
    if (StringUtils.isNotEmpty(query) && !query.startsWith("--")) {
      try {
        session.execute(query);
      }
      catch (InvalidQueryException e) {
        if (e.getMessage().contains("an existing column")) {
          LOGGER.info("Table was already altered.");
        }
        else {
          throw e;
        }
      }
    }
  }

  /**
   * Execute the given update query. Return value useful when using an IF clause (ie: compare and set)
   */
  public boolean executeUpdate(PreparedStatement preparedStatement, Object... values) {
    ResultSet resultSet = session.execute(preparedStatement.bind(values));
    Row row = resultSet.one();
    return row.getBool("[applied]");
  }

  /**
   * Execute the given update/insert query (ie: no select queries) with the given values.
   *
   * @param query
   */
  public void execute(String query, Object... values) {
    session.execute(query, values);
  }

  /**
   * Execute a PreparedStatement holding any update/insert query (ie: no select queries) with the given binding values.
   */
  public void execute(PreparedStatement preparedStatement, Object... values) {
    session.execute(preparedStatement.bind(values));
  }

  public void execute(String query, ConsistencyLevel consistencyLevel) {
    PreparedStatement statement = prepare(query, consistencyLevel);
    execute(statement);
  }

  /**
   * Prepare a statement bound to be used multiple times with different parameters
   *
   * @param query
   */
  public PreparedStatement prepare(String query, ConsistencyLevel consistencyLevel) {
    PreparedStatement preparedStatement = session.prepare(query);
    preparedStatement.setConsistencyLevel(consistencyLevel);
    return preparedStatement;
  }

  public ResultSet selectWithPagination(SimpleStatement simpleStatement) {
    simpleStatement.setFetchSize(2000);
    return session.execute(simpleStatement);
  }

  public String getCurrentKeyspace() {
    return this.session.getLoggedKeyspace();
  }

  public ConsistencyLevel getWriteConsistencyLevel() {
    return WRITE_CONSISTENCY_LEVEL;
  }

  public ConsistencyLevel getReadConsistencyLevel() {
    return READ_CONSISTENCY_LEVEL;
  }
}