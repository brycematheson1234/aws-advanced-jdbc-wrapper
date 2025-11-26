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

package software.amazon.jdbc.plugin.strategy.poolagnosticleastconnections;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * A connection plugin that tracks active connections per host and provides
 * a "poolAgnosticLeastConnections" host selection strategy.
 *
 * <p>This plugin intercepts connect() calls to increment the connection count
 * and wraps connections to track when they are closed to decrement the count.</p>
 *
 * <p>Usage: Set the property "readerInitialConnectionHostSelectorStrategy" to
 * "poolAgnosticLeastConnections" to use this strategy for reader connections.</p>
 */
public class PoolAgnosticLeastConnectionsPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(PoolAgnosticLeastConnectionsPlugin.class.getName());

  public static final String STRATEGY_NAME = "poolAgnosticLeastConnections";

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
          add("acceptsStrategy");
          add("getHostSpecByStrategy");
        }
      });

  protected final @NonNull PluginService pluginService;
  protected final @NonNull Properties properties;
  protected final @NonNull PoolAgnosticConnectionTracker tracker;

  public PoolAgnosticLeastConnectionsPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties properties) {
    this(pluginService, properties, PoolAgnosticConnectionTracker.getInstance());
  }

  public PoolAgnosticLeastConnectionsPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull PoolAgnosticConnectionTracker tracker) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.tracker = tracker;
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
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final Connection conn = connectFunc.call();
    if (conn != null && hostSpec != null) {
      final String hostUrl = hostSpec.getUrl();
      tracker.incrementConnectionCount(hostUrl);
      LOGGER.finest(() -> String.format(
          "Incremented connection count for host %s. Current count: %d",
          hostUrl, tracker.getConnectionCount(hostUrl)));

      return new TrackedConnectionWrapper(conn, hostUrl, tracker, this.pluginService);
    }
    return conn;
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {

    final Connection conn = forceConnectFunc.call();
    if (conn != null && hostSpec != null) {
      final String hostUrl = hostSpec.getUrl();
      tracker.incrementConnectionCount(hostUrl);
      LOGGER.finest(() -> String.format(
          "Incremented connection count for host %s (forceConnect). Current count: %d",
          hostUrl, tracker.getConnectionCount(hostUrl)));

      return new TrackedConnectionWrapper(conn, hostUrl, tracker, this.pluginService);
    }
    return conn;
  }

  @Override
  public boolean acceptsStrategy(final HostRole role, final String strategy) {
    return STRATEGY_NAME.equalsIgnoreCase(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(final HostRole role, final String strategy)
      throws SQLException, UnsupportedOperationException {
    return getHostSpecByStrategy(this.pluginService.getHosts(), role, strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      final List<HostSpec> hosts,
      final HostRole role,
      final String strategy)
      throws SQLException, UnsupportedOperationException {

    if (!acceptsStrategy(role, strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "HostSelector.unsupportedStrategy",
              new Object[] {strategy}));
    }

    final List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec ->
            role.equals(hostSpec.getRole())
                && hostSpec.getAvailability().equals(HostAvailability.AVAILABLE))
        .sorted(Comparator.comparingInt(hostSpec -> tracker.getConnectionCount(hostSpec.getUrl())))
        .collect(Collectors.toList());

    if (eligibleHosts.isEmpty()) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[] {role}));
    }

    final HostSpec selectedHost = eligibleHosts.get(0);
    LOGGER.finest(() -> String.format(
        "Selected host %s with %d active connections",
        selectedHost.getUrl(), tracker.getConnectionCount(selectedHost.getUrl())));

    return selectedHost;
  }

  public static void clearCache() {
    PoolAgnosticConnectionTracker.getInstance().clear();
  }
}
