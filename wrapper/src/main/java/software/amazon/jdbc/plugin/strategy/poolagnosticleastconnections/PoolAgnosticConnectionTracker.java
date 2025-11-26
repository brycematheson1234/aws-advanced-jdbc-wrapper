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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe singleton that tracks active connection counts per host URL.
 * This tracker provides pool-agnostic connection counting without requiring
 * Hikari or any other connection pool.
 */
public final class PoolAgnosticConnectionTracker {

  private static final PoolAgnosticConnectionTracker INSTANCE = new PoolAgnosticConnectionTracker();

  private final ConcurrentHashMap<String, AtomicInteger> connectionCounts = new ConcurrentHashMap<>();

  private PoolAgnosticConnectionTracker() {
  }

  public static PoolAgnosticConnectionTracker getInstance() {
    return INSTANCE;
  }

  /**
   * Increments the connection count for the given host URL.
   *
   * @param hostUrl the host URL (e.g., "host:port")
   */
  public void incrementConnectionCount(final String hostUrl) {
    if (hostUrl == null) {
      return;
    }
    connectionCounts.computeIfAbsent(hostUrl, k -> new AtomicInteger(0)).incrementAndGet();
  }

  /**
   * Decrements the connection count for the given host URL.
   * The count will not go below zero.
   *
   * @param hostUrl the host URL (e.g., "host:port")
   */
  public void decrementConnectionCount(final String hostUrl) {
    if (hostUrl == null) {
      return;
    }
    connectionCounts.computeIfPresent(hostUrl, (k, v) -> {
      v.updateAndGet(current -> Math.max(0, current - 1));
      return v;
    });
  }

  /**
   * Gets the current active connection count for the given host URL.
   *
   * @param hostUrl the host URL (e.g., "host:port")
   * @return the number of active connections, or 0 if the host has no tracked connections
   */
  public int getConnectionCount(final String hostUrl) {
    if (hostUrl == null) {
      return 0;
    }
    AtomicInteger count = connectionCounts.get(hostUrl);
    return count != null ? count.get() : 0;
  }

  /**
   * Returns a snapshot of all tracked hosts and their connection counts.
   * This is primarily for debugging and monitoring purposes.
   *
   * @return an unmodifiable view of the current connection counts
   */
  public Map<String, Integer> getAllConnectionCounts() {
    ConcurrentHashMap<String, Integer> snapshot = new ConcurrentHashMap<>();
    connectionCounts.forEach((key, value) -> snapshot.put(key, value.get()));
    return snapshot;
  }

  /**
   * Clears all tracked connection counts.
   * This should only be used for testing or shutdown scenarios.
   */
  public void clear() {
    connectionCounts.clear();
  }
}
