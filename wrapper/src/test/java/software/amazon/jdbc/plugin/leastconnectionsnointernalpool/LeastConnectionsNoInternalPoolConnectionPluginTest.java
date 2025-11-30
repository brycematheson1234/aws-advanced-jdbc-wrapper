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

package software.amazon.jdbc.plugin.leastconnectionsnointernalpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.LeastConnectionsNoInternalPoolHostSelector;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class LeastConnectionsNoInternalPoolConnectionPluginTest {

  private static final int TEST_PORT = 5432;
  private static final Properties EMPTY_PROPERTIES = new Properties();

  @Mock
  private PluginService mockPluginService;
  @Mock
  private Connection mockConnection;
  @Mock
  private JdbcCallable<Connection, SQLException> mockConnectFunc;
  @Mock
  private JdbcCallable<Void, SQLException> mockCloseFunc;

  private AutoCloseable closeable;
  private LeastConnectionsNoInternalPoolConnectionPlugin plugin;
  private HostSpec hostSpec;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    plugin = new LeastConnectionsNoInternalPoolConnectionPlugin(mockPluginService, EMPTY_PROPERTIES);
    hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("test-host")
        .port(TEST_PORT)
        .build();
    LeastConnectionsNoInternalPoolHostSelector.clearCache();
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
    LeastConnectionsNoInternalPoolHostSelector.clearCache();
  }

  @Test
  void testConnect_IncrementsCounter() throws SQLException {
    when(mockConnectFunc.call()).thenReturn(mockConnection);

    Connection result = plugin.connect("jdbc:postgresql://", hostSpec, EMPTY_PROPERTIES, true, mockConnectFunc);

    assertSame(mockConnection, result);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
  }

  @Test
  void testConnect_NullConnection_DoesNotIncrement() throws SQLException {
    when(mockConnectFunc.call()).thenReturn(null);

    Connection result = plugin.connect("jdbc:postgresql://", hostSpec, EMPTY_PROPERTIES, true, mockConnectFunc);

    assertEquals(null, result);
    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
  }

  @Test
  void testConnect_Exception_DoesNotIncrement() throws SQLException {
    when(mockConnectFunc.call()).thenThrow(new SQLException("Connection failed"));

    assertThrows(SQLException.class, () ->
        plugin.connect("jdbc:postgresql://", hostSpec, EMPTY_PROPERTIES, true, mockConnectFunc));

    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
  }

  @Test
  void testForceConnect_IncrementsCounter() throws SQLException {
    when(mockConnectFunc.call()).thenReturn(mockConnection);

    Connection result = plugin.forceConnect("jdbc:postgresql://", hostSpec, EMPTY_PROPERTIES, true, mockConnectFunc);

    assertSame(mockConnection, result);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
  }

  @Test
  void testExecuteClose_DecrementsCounter() throws Exception {
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(hostSpec.getHostAndPort(), mockConnection);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));

    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(mockCloseFunc.call()).thenReturn(null);

    plugin.execute(
        Void.class,
        SQLException.class,
        mockConnection,
        JdbcMethod.CONNECTION_CLOSE.methodName,
        mockCloseFunc,
        new Object[]{});

    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
    verify(mockCloseFunc).call();
  }

  @Test
  void testExecuteAbort_DecrementsCounter() throws Exception {
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(hostSpec.getHostAndPort(), mockConnection);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));

    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    when(mockCloseFunc.call()).thenReturn(null);

    plugin.execute(
        Void.class,
        SQLException.class,
        mockConnection,
        JdbcMethod.CONNECTION_ABORT.methodName,
        mockCloseFunc,
        new Object[]{});

    assertEquals(0, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
    verify(mockCloseFunc).call();
  }

  @Test
  void testExecuteOtherMethod_DoesNotDecrementCounter() throws Exception {
    LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(hostSpec.getHostAndPort(), mockConnection);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));

    when(mockPluginService.getCurrentConnection()).thenReturn(mockConnection);
    when(mockPluginService.getCurrentHostSpec()).thenReturn(hostSpec);
    JdbcCallable<Boolean, SQLException> mockAutoCommitFunc = () -> true;

    Boolean result = plugin.execute(
        Boolean.class,
        SQLException.class,
        mockConnection,
        JdbcMethod.CONNECTION_GETAUTOCOMMIT.methodName,
        mockAutoCommitFunc,
        new Object[]{});

    assertEquals(true, result);
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort()));
  }

  @Test
  void testMultipleConnections_TrackedIndependently() throws SQLException {
    HostSpec host1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host-1")
        .port(TEST_PORT)
        .build();
    HostSpec host2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("host-2")
        .port(TEST_PORT)
        .build();

    Connection mockConn1 = org.mockito.Mockito.mock(Connection.class);
    Connection mockConn2 = org.mockito.Mockito.mock(Connection.class);

    when(mockConnectFunc.call()).thenReturn(mockConn1).thenReturn(mockConn2);

    plugin.connect("jdbc:postgresql://", host1, EMPTY_PROPERTIES, true, mockConnectFunc);
    plugin.connect("jdbc:postgresql://", host2, EMPTY_PROPERTIES, false, mockConnectFunc);

    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(host1.getHostAndPort()));
    assertEquals(1, LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(host2.getHostAndPort()));
  }

  @Test
  void testGetSubscribedMethods() {
    java.util.Set<String> methods = plugin.getSubscribedMethods();
    assertEquals(4, methods.size());
    assertEquals(true, methods.contains(JdbcMethod.CONNECT.methodName));
    assertEquals(true, methods.contains(JdbcMethod.FORCECONNECT.methodName));
    assertEquals(true, methods.contains(JdbcMethod.CONNECTION_CLOSE.methodName));
    assertEquals(true, methods.contains(JdbcMethod.CONNECTION_ABORT.methodName));
  }
}
