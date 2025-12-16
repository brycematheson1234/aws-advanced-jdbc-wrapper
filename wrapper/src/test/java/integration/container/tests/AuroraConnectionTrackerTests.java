/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPoolConfigurator;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.LogQueryConnectionPlugin;

/**
 * Integration tests for AuroraConnectionTrackerPlugin to verify alias query behavior
 * when internal connection pooling is enabled.
 *
 * <p>This test demonstrates a bug where the AuroraConnectionTrackerPlugin executes
 * redundant alias queries (SELECT CONCAT(@@hostname, ':', @@port)) on every 
 * getConnection() call when internal pooling is enabled.
 *
 * <p>The bug occurs because:
 * <ul>
 *   <li>Internal pooling (HikariPooledConnectionProvider) causes the plugin chain to run
 *       on every getConnection() borrow</li>
 *   <li>AuroraConnectionTrackerPlugin.connect() unconditionally calls resetAliases() and
 *       fillAliases() for cluster endpoints (isRdsCluster()=true) or non-RDS hosts (OTHER)</li>
 *   <li>This results in repeated SQL queries even when the same physical Connection is reused</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(17)
public class AuroraConnectionTrackerTests {

  private static final Logger LOGGER = Logger.getLogger(AuroraConnectionTrackerTests.class.getName());

  /**
   * Test that demonstrates the redundant alias query bug.
   *
   * <p>When internal pooling is enabled, alias queries should only be executed once 
   * per physical connection, not on every logical borrow from the pool.
   *
   * <p>This test:
   * <ol>
   *   <li>Sets up internal pooling with maxPoolSize=1 to force connection reuse</li>
   *   <li>Enables logQuery plugin to capture all SQL statements</li>
   *   <li>Makes 5 getConnection() calls, each returning the same pooled connection</li>
   *   <li>Counts the number of alias queries (@@hostname) in logs</li>
   *   <li>Asserts that the bug exists: alias queries run on every borrow (5 times instead of 1)</li>
   * </ol>
   */
  @TestTemplate
  public void test_aliasQueriesRunOnEveryGetConnectionWithInternalPooling(TestDriver testDriver)
      throws SQLException, UnsupportedEncodingException {

    LOGGER.info("Running test with driver: " + testDriver);

    // Set up log capture to observe SQL queries
    Logger rootLogger = Logger.getLogger("");
    ByteArrayOutputStream logOutputStream = new ByteArrayOutputStream();
    StreamHandler logHandler = new StreamHandler(logOutputStream, new SimpleFormatter());
    logHandler.setLevel(Level.ALL);
    rootLogger.addHandler(logHandler);

    // Configure internal connection pooling with pool size of 1 to force reuse
    final HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider(getHikariConfig(1));
    Driver.setCustomConnectionProvider(provider);

    try {
      Properties props = getProps();
      String url = ConnectionStringHelper.getWrapperUrl(
          testDriver,
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getHost(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getPort(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      LOGGER.info("Connecting to: " + url);

      final int numConnections = 5;

      // Make multiple getConnection() calls - same physical connection should be reused
      for (int i = 0; i < numConnections; i++) {
        try (Connection conn = DriverManager.getConnection(url, props)) {
          assertTrue(conn.isValid(5));
          // Execute a simple query to verify connection works
          try (Statement stmt = conn.createStatement();
               ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
          }
          LOGGER.fine("Connection " + i + " successful");
        }
      }

      // Flush and capture log output
      logHandler.flush();
      String logMessages = logOutputStream.toString("UTF-8");

      // Count alias queries in the logs
      // The alias queries contain pattern "@@hostname" (MySQL) or similar
      int aliasQueryCount = countAliasQueries(logMessages);

      LOGGER.info("Number of getConnection() calls: " + numConnections);
      LOGGER.info("Number of alias query executions: " + aliasQueryCount);

      // BUG DEMONSTRATION:
      // With the bug present, alias queries run on every getConnection() call,
      // so we expect aliasQueryCount >= numConnections (at least 5).
      //
      // After fix, alias queries should only run once per physical connection,
      // so we would expect aliasQueryCount == 1.
      //
      // This test PASSES if the bug exists (to demonstrate the bug).
      // Once the bug is fixed, this test should be updated to assert aliasQueryCount == 1.
      assertTrue(
          aliasQueryCount >= numConnections,
          String.format(
              "Expected alias queries to run at least %d times (once per getConnection call) "
                  + "demonstrating the bug, but found only %d executions. "
                  + "If the bug has been fixed, update this test to expect aliasQueryCount == 1.",
              numConnections,
              aliasQueryCount));

      LOGGER.info("Bug confirmed: Alias queries executed " + aliasQueryCount
          + " times for " + numConnections + " getConnection() calls with pooled connection reuse.");

    } finally {
      rootLogger.removeHandler(logHandler);
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  /**
   * Counts the number of alias query executions in the log messages.
   * Alias queries contain patterns like "@@hostname" (MySQL) or "inet_server_addr" (PostgreSQL).
   */
  private int countAliasQueries(String logMessages) {
    int count = 0;

    // Pattern for MySQL hostname query - looks for @@hostname in query logs
    Pattern mysqlPattern = Pattern.compile("@@hostname", Pattern.CASE_INSENSITIVE);
    Matcher mysqlMatcher = mysqlPattern.matcher(logMessages);
    while (mysqlMatcher.find()) {
      count++;
    }

    // Pattern for PostgreSQL - inet_server_addr
    Pattern pgPattern = Pattern.compile("inet_server_addr", Pattern.CASE_INSENSITIVE);
    Matcher pgMatcher = pgPattern.matcher(logMessages);
    while (pgMatcher.find()) {
      count++;
    }

    return count;
  }

  private Properties getProps() {
    Properties props = ConnectionStringHelper.getDefaultProperties();
    // Enable auroraConnectionTracker (triggers the bug) and logQuery (to observe SQL)
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraConnectionTracker,logQuery");
    props.setProperty(LogQueryConnectionPlugin.ENHANCED_LOG_QUERY_ENABLED.name, "true");
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");
    return props;
  }

  private HikariPoolConfigurator getHikariConfig(int maxPoolSize) {
    return (hostSpec, props) -> {
      final HikariConfig config = new HikariConfig();
      config.setMaximumPoolSize(maxPoolSize);
      config.setMinimumIdle(1);
      config.setInitializationFailTimeout(75000);
      return config;
    };
  }
}
