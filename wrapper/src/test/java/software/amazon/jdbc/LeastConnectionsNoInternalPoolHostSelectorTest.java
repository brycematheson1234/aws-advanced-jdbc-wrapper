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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class LeastConnectionsNoInternalPoolHostSelectorTest {

  private static final int TEST_PORT = 5432;
  private LeastConnectionsNoInternalPoolHostSelector selector;
  private Properties props;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer-host")
      .port(TEST_PORT)
      .role(HostRole.WRITER)
      .build();

  private final HostSpec readerHost1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-host-1")
      .port(TEST_PORT)
      .role(HostRole.READER)
      .build();

  private final HostSpec readerHost2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-host-2")
      .port(TEST_PORT)
      .role(HostRole.READER)
      .build();

  private final HostSpec readerHost3 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-host-3")
      .port(TEST_PORT)
      .role(HostRole.READER)
      .build();

  private final HostSpec unavailableHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("unavailable-host")
      .port(TEST_PORT)
      .role(HostRole.READER)
      .availability(HostAvailability.NOT_AVAILABLE)
      .build();

  @BeforeEach
  void setUp() {
    selector = new LeastConnectionsNoInternalPoolHostSelector();
    props = new Properties();
    LeastConnectionsNoInternalPoolHostSelector.clearCache();
  }

  @AfterEach
  void tearDown() {
    LeastConnectionsNoInternalPoolHostSelector.clearCache();
  }

  @Test
  void testGetHost_NoHostsMatchingRole() {
    List<HostSpec> hosts = Collections.singletonList(writerHost);
    assertThrows(SQLException.class, () -> selector.getHost(hosts, HostRole.READER, props));
  }

  @Test
  void testGetHost_SelectsAvailableHostOverUnavailable() throws SQLException {
    List<HostSpec> hosts = Arrays.asList(unavailableHost, readerHost1);
    HostSpec selected = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost1, selected);
  }

  @Test
  void testGetHost_SelectsHostWithLeastConnections() throws SQLException {
    List<HostSpec> hosts = Arrays.asList(readerHost1, readerHost2, readerHost3);

    Connection mockConn1 = mock(Connection.class);
    Connection mockConn2 = mock(Connection.class);
    Connection mockConn3 = mock(Connection.class);
    Connection mockConn4 = mock(Connection.class);
    Connection mockConn5 = mock(Connection.class);

    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
        readerHost1.getHostAndPort(), mockConn1);
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
        readerHost1.getHostAndPort(), mockConn2);
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
        readerHost1.getHostAndPort(), mockConn3);

    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
        readerHost2.getHostAndPort(), mockConn4);
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
        readerHost2.getHostAndPort(), mockConn5);

    HostSpec selected = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost3, selected);
  }

  @Test
  void testGetHost_EqualConnectionsReturnsFirstEligible() throws SQLException {
    List<HostSpec> hosts = Arrays.asList(readerHost1, readerHost2, readerHost3);
    HostSpec selected = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost1, selected);
  }

  @Test
  void testIncrementDecrement() {
    Connection mockConn = mock(Connection.class);
    String hostAndPort = readerHost1.getHostAndPort();

    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));

    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(hostAndPort, mockConn);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));

    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(hostAndPort, mock(Connection.class));
    assertEquals(2, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));

    LeastConnectionsNoInternalPoolHostSelector.decrementConnectionCount(mockConn);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));
  }

  @Test
  void testDecrementNeverGoesBelowZero() {
    Connection mockConn = mock(Connection.class);
    String hostAndPort = readerHost1.getHostAndPort();

    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(hostAndPort, mockConn);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));

    LeastConnectionsNoInternalPoolHostSelector.decrementConnectionCount(mockConn);
    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));

    LeastConnectionsNoInternalPoolHostSelector.decrementConnectionCount(mock(Connection.class));
    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostAndPort));
  }

  @Test
  void testConcurrentAccess() throws InterruptedException, SQLException {
    List<HostSpec> hosts = Arrays.asList(readerHost1, readerHost2, readerHost3);
    int numThreads = 30;
    int connectionsPerThread = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    Map<String, Integer> selectionCounts = Collections.synchronizedMap(new HashMap<>());

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          for (int j = 0; j < connectionsPerThread; j++) {
            HostSpec selected = selector.getHost(hosts, HostRole.READER, props);
            Connection mockConn = mock(Connection.class);
            LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
                selected.getHostAndPort(), mockConn);
            selectionCounts.merge(selected.getHostAndPort(), 1, Integer::sum);
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    int totalConnections = numThreads * connectionsPerThread;
    int expectedPerHost = totalConnections / 3;
    int tolerance = (int) (expectedPerHost * 0.3);

    for (HostSpec host : hosts) {
      int count = LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(host.getHostAndPort());
      assertTrue(count >= expectedPerHost - tolerance && count <= expectedPerHost + tolerance,
          String.format("Host %s has %d connections, expected around %d (±%d)",
              host.getHost(), count, expectedPerHost, tolerance));
    }
  }

  @Test
  void testClearCache() {
    Connection mockConn = mock(Connection.class);
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
        readerHost1.getHostAndPort(), mockConn);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(readerHost1.getHostAndPort()));

    LeastConnectionsNoInternalPoolHostSelector.clearCache();
    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(readerHost1.getHostAndPort()));
  }

  @Test
  void testRebalancingAfterCloseConnections() throws SQLException {
    List<HostSpec> hosts = Arrays.asList(readerHost1, readerHost2);

    List<Connection> host1Connections = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Connection mockConn = mock(Connection.class);
      host1Connections.add(mockConn);
      LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
          readerHost1.getHostAndPort(), mockConn);
    }

    HostSpec selected = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost2, selected);

    for (Connection conn : host1Connections) {
      LeastConnectionsNoInternalPoolHostSelector.decrementConnectionCount(conn);
    }

    selected = selector.getHost(hosts, HostRole.READER, props);
    assertEquals(readerHost1, selected);
  }
}
