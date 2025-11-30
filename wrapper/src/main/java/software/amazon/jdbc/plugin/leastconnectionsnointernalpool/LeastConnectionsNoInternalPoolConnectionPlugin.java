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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.LeastConnectionsNoInternalPoolHostSelector;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;

/**
 * A connection plugin that tracks active connections per host for use with
 * {@link LeastConnectionsNoInternalPoolHostSelector}.
 *
 * <p>This plugin intercepts connect, close, and abort operations to maintain
 * accurate connection counts without relying on internal connection pools.</p>
 */
public class LeastConnectionsNoInternalPoolConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(LeastConnectionsNoInternalPoolConnectionPlugin.class.getName());

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECT.methodName);
          add(JdbcMethod.FORCECONNECT.methodName);
          add(JdbcMethod.CONNECTION_CLOSE.methodName);
          add(JdbcMethod.CONNECTION_ABORT.methodName);
        }
      });

  protected final PluginService pluginService;
  protected final Properties properties;

  public LeastConnectionsNoInternalPoolConnectionPlugin(
      final PluginService pluginService,
      final Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    Connection connection = connectFunc.call();

    if (connection != null) {
      LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
          hostSpec.getHostAndPort(), connection);
      LOGGER.finest(() -> String.format(
          "Incremented connection count for %s, new count: %d",
          hostSpec.getHostAndPort(),
          LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort())));
    }

    return connection;
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc) throws SQLException {

    Connection connection = forceConnectFunc.call();

    if (connection != null) {
      LeastConnectionsNoInternalPoolHostSelector.incrementConnectionCount(
          hostSpec.getHostAndPort(), connection);
      LOGGER.finest(() -> String.format(
          "Incremented connection count for %s (forceConnect), new count: %d",
          hostSpec.getHostAndPort(),
          LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort())));
    }

    return connection;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs) throws E {

    if (JdbcMethod.CONNECTION_CLOSE.methodName.equals(methodName)
        || JdbcMethod.CONNECTION_ABORT.methodName.equals(methodName)) {
      Connection currentConnection = this.pluginService.getCurrentConnection();
      if (currentConnection != null) {
        LeastConnectionsNoInternalPoolHostSelector.decrementConnectionCount(currentConnection);
        HostSpec hostSpec = this.pluginService.getCurrentHostSpec();
        if (hostSpec != null) {
          LOGGER.finest(() -> String.format(
              "Decremented connection count for %s (%s), new count: %d",
              hostSpec.getHostAndPort(),
              methodName,
              LeastConnectionsNoInternalPoolHostSelector.getConnectionCount(hostSpec.getHostAndPort())));
        }
      }
    }

    return jdbcMethodFunc.call();
  }
}
