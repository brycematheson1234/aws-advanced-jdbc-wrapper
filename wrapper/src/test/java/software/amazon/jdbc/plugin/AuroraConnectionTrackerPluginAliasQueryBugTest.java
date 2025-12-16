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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Test that demonstrates a performance bug where resetAliases() and fillAliases() are called
 * on EVERY getConnection() when using internal connection pooling (HikariPooledConnectionProvider)
 * with cluster endpoints, even when borrowing an existing connection from the pool.
 *
 * <p><b>KEY DISTINCTION - When the bug manifests:</b>
 * <ul>
 *   <li><b>External pooling</b> (HikariCP wrapping AwsWrapperDataSource): Plugin chain only runs
 *       when HikariCP creates a NEW physical connection. Subsequent borrows from HikariCP don't
 *       invoke the plugin chain at all, so no extra queries. This is the STANDARD configuration.</li>
 *   <li><b>Internal pooling</b> (HikariPooledConnectionProvider): The plugin chain runs on EVERY
 *       getConnection() call, even when borrowing from the internal pool. This is used with
 *       customEndpoint + initialConnection plugins for leastConnections load balancing.</li>
 * </ul>
 *
 * <p><b>The bug:</b> AuroraConnectionTrackerPlugin.connect() unconditionally calls:
 * <pre>
 *   if (type.isRdsCluster() || type == RdsUrlType.OTHER || type == RdsUrlType.IP_ADDRESS) {
 *     hostSpec.resetAliases();
 *     this.pluginService.fillAliases(conn, hostSpec);
 *   }
 * </pre>
 * There is no guard to skip alias queries when reusing the same physical connection.
 * Although PluginServiceImpl.fillAliases() has a check {@code if (!hostSpec.getAliases().isEmpty())},
 * it is defeated by the preceding resetAliases() call which always clears the cache.
 *
 * <p><b>How this test simulates internal pooling:</b> We call {@code plugin.connect()} multiple
 * times with the SAME {@code Connection} instance (returned by connectFunc) and the SAME
 * {@code HostSpec}. This mirrors what happens when HikariPooledConnectionProvider returns
 * a pooled physical connection on repeated getConnection() calls.
 *
 * <p><b>Impact:</b> Extra SQL queries (@@hostname:@@port, @@aurora_server_id) per getConnection()
 * call when using internal pooling with cluster endpoints. In practice this is 4-5 queries;
 * this test models a single representative query for simplicity.
 *
 * <p><b>Expected behavior:</b> Alias queries should only execute when a NEW physical connection
 * is created, not when borrowing an existing connection from internal pool.
 */
class AuroraConnectionTrackerPluginAliasQueryBugTest {

  private static final String CUSTOM_CLUSTER_HOST =
      "my-cluster.cluster-custom-xyz123.us-east-1.rds.amazonaws.com";
  private static final int PORT = 5432;
  private static final String PROTOCOL = "jdbc:postgresql://";

  @Mock Connection mockConnection;
  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock PluginService mockPluginService;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  @Mock OpenedConnectionTracker mockTracker;

  private AutoCloseable closeable;
  private final AtomicInteger fillAliasesCallCount = new AtomicInteger(0);
  private final AtomicInteger aliasQueryExecutionCount = new AtomicInteger(0);

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockConnection.isValid(any(Integer.class))).thenReturn(true);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenAnswer(invocation -> {
      aliasQueryExecutionCount.incrementAndGet();
      return mockResultSet;
    });
    when(mockResultSet.next()).thenReturn(false);

    when(mockDialect.getHostAliasQuery()).thenReturn("SELECT CONCAT(@@hostname, ':', @@port)");
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());

    doAnswer(invocation -> {
      fillAliasesCallCount.incrementAndGet();
      Connection conn = invocation.getArgument(0);
      HostSpec hostSpec = invocation.getArgument(1);

      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(mockDialect.getHostAliasQuery())) {
          while (rs.next()) {
            hostSpec.addAlias(rs.getString(1));
          }
        }
      }
      return null;
    }).when(mockPluginService).fillAliases(any(Connection.class), any(HostSpec.class));
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
    fillAliasesCallCount.set(0);
    aliasQueryExecutionCount.set(0);
  }

  /**
   * This test demonstrates the bug where alias queries are executed on EVERY getConnection()
   * call when using internal connection pooling (HikariPooledConnectionProvider) with
   * cluster endpoints.
   *
   * <p><b>Scenario:</b> Using customEndpoint + initialConnection plugins with internal pooling.
   * The hostSpec URL remains the CLUSTER endpoint (cluster-custom-xxx) throughout, even though
   * HikariPooledConnectionProvider internally pools connections to specific instances.
   *
   * <p><b>Bug mechanism:</b>
   * <ol>
   *   <li>Application calls getConnection()</li>
   *   <li>Plugin chain invokes AuroraConnectionTrackerPlugin.connect()</li>
   *   <li>hostSpec.getHost() = cluster endpoint URL → isRdsCluster()=true</li>
   *   <li>resetAliases() clears any cached aliases</li>
   *   <li>fillAliases() executes SQL queries to re-identify the host</li>
   *   <li>HikariPooledConnectionProvider returns pooled connection (no new physical connection)</li>
   *   <li>Repeat on EVERY getConnection() call</li>
   * </ol>
   *
   * <p><b>Why standard config doesn't have this bug:</b> With external HikariCP pooling,
   * the plugin chain only runs when creating NEW physical connections. HikariCP's borrow
   * operation doesn't invoke the AWS wrapper at all.
   */
  @Test
  void testAliasQueriesExecutedOnEveryGetConnectionWithInternalPoolingAndClusterEndpoint() throws SQLException {
    HostSpec customClusterHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(CUSTOM_CLUSTER_HOST)
        .port(PORT)
        .build();

    RdsUtils rdsUtils = new RdsUtils();
    RdsUrlType urlType = rdsUtils.identifyRdsType(CUSTOM_CLUSTER_HOST);
    assertEquals(RdsUrlType.RDS_CUSTOM_CLUSTER, urlType);
    assertTrue(urlType.isRdsCluster());

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        new Properties(),
        rdsUtils,
        mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    int numberOfGetConnectionCalls = 5;
    for (int i = 0; i < numberOfGetConnectionCalls; i++) {
      Connection conn = plugin.connect(
          PROTOCOL,
          customClusterHostSpec,
          new Properties(),
          i == 0,
          connectFunc);
      assertEquals(mockConnection, conn);
    }

    verify(mockPluginService, times(numberOfGetConnectionCalls))
        .fillAliases(eq(mockConnection), eq(customClusterHostSpec));
    assertEquals(numberOfGetConnectionCalls, fillAliasesCallCount.get());
    assertEquals(numberOfGetConnectionCalls, aliasQueryExecutionCount.get());
  }

  /**
   * This test shows that for RDS instance endpoints (not cluster endpoints),
   * the alias queries are NOT executed, which is the correct behavior.
   *
   * <p>This highlights that the bug specifically affects cluster endpoints
   * (including custom cluster endpoints).
   */
  @Test
  void testInstanceEndpointDoesNotTriggerAliasQueries() throws SQLException {
    String instanceHost = "my-instance.xyz123.us-east-1.rds.amazonaws.com";
    HostSpec instanceHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(instanceHost)
        .port(PORT)
        .build();

    RdsUtils rdsUtils = new RdsUtils();
    RdsUrlType urlType = rdsUtils.identifyRdsType(instanceHost);
    assertEquals(RdsUrlType.RDS_INSTANCE, urlType);

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        new Properties(),
        rdsUtils,
        mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    int numberOfGetConnectionCalls = 5;
    for (int i = 0; i < numberOfGetConnectionCalls; i++) {
      Connection conn = plugin.connect(
          PROTOCOL,
          instanceHostSpec,
          new Properties(),
          i == 0,
          connectFunc);
      assertEquals(mockConnection, conn);
    }

    verify(mockPluginService, times(0)).fillAliases(any(), any());
    assertEquals(0, fillAliasesCallCount.get());
    assertEquals(0, aliasQueryExecutionCount.get());
  }

  /**
   * This test demonstrates the expected behavior after the bug is fixed.
   *
   * <p><b>Expected behavior with internal pooling:</b>
   * <ul>
   *   <li>First getConnection() to a new host → creates physical connection → fillAliases called</li>
   *   <li>Subsequent getConnection() calls → borrow from pool → fillAliases should NOT be called</li>
   * </ul>
   *
   * <p><b>Current behavior (bug):</b> fillAliases is called on EVERY getConnection() because
   * resetAliases() unconditionally clears the cache; there is no guard to skip alias queries
   * when reusing the same physical connection instance.
   *
   * <p><b>Potential fix:</b> AuroraConnectionTrackerPlugin should check if this is a pooled
   * connection borrow (same physical connection) vs a new physical connection, and skip
   * alias queries for pooled borrows. Alternatively, the internal pooling architecture could
   * avoid routing pool borrows through the full plugin chain.
   */
  @Test
  void testAliasQueriesShouldOnlyExecuteOncePerPhysicalConnection_BUG_DEMONSTRATION() throws SQLException {
    HostSpec customClusterHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(CUSTOM_CLUSTER_HOST)
        .port(PORT)
        .build();

    RdsUtils rdsUtils = new RdsUtils();

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        new Properties(),
        rdsUtils,
        mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    int numberOfGetConnectionCalls = 5;
    for (int i = 0; i < numberOfGetConnectionCalls; i++) {
      Connection conn = plugin.connect(
          PROTOCOL,
          customClusterHostSpec,
          new Properties(),
          i == 0,
          connectFunc);
      assertEquals(mockConnection, conn);
    }

    assertEquals(
        numberOfGetConnectionCalls,
        fillAliasesCallCount.get(),
        "BUG DEMONSTRATED: fillAliases() was called " + fillAliasesCallCount.get() +
            " times for " + numberOfGetConnectionCalls + " plugin.connect() calls with the SAME " +
            "Connection instance. Expected: 1 (only for the first call with a new physical connection). " +
            "This shows that alias queries (@@hostname:@@port, @@aurora_server_id) " +
            "are being executed on EVERY getConnection(), causing unnecessary database round-trips.");

    assertEquals(
        numberOfGetConnectionCalls,
        aliasQueryExecutionCount.get(),
        "BUG DEMONSTRATED: Alias SQL query was executed " + aliasQueryExecutionCount.get() +
            " times for the same Connection instance. Expected: 1. " +
            "Each execution represents unnecessary network latency.");
  }

  /**
   * Tests that writer cluster endpoints also trigger alias queries on every plugin.connect() call.
   *
   * <p><b>Note:</b> This bug does NOT manifest with standard external HikariCP pooling because
   * HikariCP only invokes the AWS wrapper when creating new physical connections. This test
   * only demonstrates the plugin behavior in isolation - the bug requires internal pooling
   * (HikariPooledConnectionProvider) to manifest in practice.
   */
  @Test
  void testWriterClusterEndpointTriggersAliasQueriesOnEveryPluginConnectCall() throws SQLException {
    String writerClusterHost = "my-cluster.cluster-xyz123.us-east-1.rds.amazonaws.com";
    HostSpec writerClusterHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(writerClusterHost)
        .port(PORT)
        .build();

    RdsUtils rdsUtils = new RdsUtils();
    RdsUrlType urlType = rdsUtils.identifyRdsType(writerClusterHost);
    assertEquals(RdsUrlType.RDS_WRITER_CLUSTER, urlType);
    assertTrue(urlType.isRdsCluster());

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        new Properties(),
        rdsUtils,
        mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    int numberOfGetConnectionCalls = 3;
    for (int i = 0; i < numberOfGetConnectionCalls; i++) {
      Connection conn = plugin.connect(
          PROTOCOL,
          writerClusterHostSpec,
          new Properties(),
          i == 0,
          connectFunc);
      assertEquals(mockConnection, conn);
    }

    assertEquals(numberOfGetConnectionCalls, fillAliasesCallCount.get(),
        "BUG: Writer cluster endpoint triggers fillAliases on every getConnection()");
    assertEquals(numberOfGetConnectionCalls, aliasQueryExecutionCount.get());
  }

  /**
   * Tests that reader cluster endpoints also trigger alias queries on every plugin.connect() call.
   *
   * <p><b>Note:</b> This bug does NOT manifest with standard external HikariCP pooling because
   * HikariCP only invokes the AWS wrapper when creating new physical connections. This test
   * only demonstrates the plugin behavior in isolation - the bug requires internal pooling
   * (HikariPooledConnectionProvider) to manifest in practice.
   */
  @Test
  void testReaderClusterEndpointTriggersAliasQueriesOnEveryPluginConnectCall() throws SQLException {
    String readerClusterHost = "my-cluster.cluster-ro-xyz123.us-east-1.rds.amazonaws.com";
    HostSpec readerClusterHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(readerClusterHost)
        .port(PORT)
        .build();

    RdsUtils rdsUtils = new RdsUtils();
    RdsUrlType urlType = rdsUtils.identifyRdsType(readerClusterHost);
    assertEquals(RdsUrlType.RDS_READER_CLUSTER, urlType);
    assertTrue(urlType.isRdsCluster());

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService,
        new Properties(),
        rdsUtils,
        mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    int numberOfGetConnectionCalls = 3;
    for (int i = 0; i < numberOfGetConnectionCalls; i++) {
      Connection conn = plugin.connect(
          PROTOCOL,
          readerClusterHostSpec,
          new Properties(),
          i == 0,
          connectFunc);
      assertEquals(mockConnection, conn);
    }

    assertEquals(numberOfGetConnectionCalls, fillAliasesCallCount.get(),
        "BUG: Reader cluster endpoint triggers fillAliases on every getConnection()");
    assertEquals(numberOfGetConnectionCalls, aliasQueryExecutionCount.get());
  }
}
