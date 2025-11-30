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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;

/**
 * A host selector that routes connections to the host with the fewest active connections.
 * Unlike {@link LeastConnectionsHostSelector}, this implementation does not rely on
 * internal connection pools (like HikariCP) to track connection counts. Instead, it
 * maintains its own in-memory counter using atomic integers.
 *
 * <p>This is useful when you want least-connections load balancing without using a
 * connection pool, or when using a pool that doesn't expose active connection counts.</p>
 */
public class LeastConnectionsNoInternalPoolHostSelector implements HostSelector {

  public static final String STRATEGY_LEAST_CONNECTIONS_NO_INTERNAL_POOL = "leastConnectionsNoInternalPool";

  private static final Map<String, AtomicInteger> connectionCounts = new ConcurrentHashMap<>();
  private static final Map<Connection, String> connectionToHost = new ConcurrentHashMap<>();

  @Override
  public HostSpec getHost(
      @NonNull final List<HostSpec> hosts,
      @NonNull final HostRole role,
      @Nullable final Properties props) throws SQLException {

    final List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec ->
            role.equals(hostSpec.getRole()) && hostSpec.getAvailability().equals(HostAvailability.AVAILABLE))
        .collect(Collectors.toList());

    if (eligibleHosts.isEmpty()) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    synchronized (connectionCounts) {
      HostSpec leastConnectionsHost = null;
      int leastConnections = Integer.MAX_VALUE;

      for (HostSpec hostSpec : eligibleHosts) {
        int count = getConnectionCount(hostSpec.getHostAndPort());
        if (count < leastConnections) {
          leastConnections = count;
          leastConnectionsHost = hostSpec;
        }
      }

      return leastConnectionsHost;
    }
  }

  /**
   * Increments the connection count for the given host.
   * Should be called when a new connection is established.
   *
   * @param hostAndPort the host:port identifier
   * @param connection the connection to track
   */
  public static void incrementConnectionCount(final String hostAndPort, final Connection connection) {
    connectionCounts.computeIfAbsent(hostAndPort, k -> new AtomicInteger(0)).incrementAndGet();
    connectionToHost.put(connection, hostAndPort);
  }

  /**
   * Decrements the connection count for the given connection.
   * Should be called when a connection is closed.
   *
   * @param connection the connection being closed
   */
  public static void decrementConnectionCount(final Connection connection) {
    String hostAndPort = connectionToHost.remove(connection);
    if (hostAndPort != null) {
      AtomicInteger count = connectionCounts.get(hostAndPort);
      if (count != null) {
        count.updateAndGet(current -> Math.max(0, current - 1));
      }
    }
  }

  /**
   * Gets the current connection count for a host.
   *
   * @param hostAndPort the host:port identifier
   * @return the number of active connections
   */
  public static int getConnectionCount(final String hostAndPort) {
    AtomicInteger count = connectionCounts.get(hostAndPort);
    return count != null ? count.get() : 0;
  }

  /**
   * Clears all connection tracking data. Primarily used for testing.
   */
  public static void clearCache() {
    connectionCounts.clear();
    connectionToHost.clear();
  }
}
