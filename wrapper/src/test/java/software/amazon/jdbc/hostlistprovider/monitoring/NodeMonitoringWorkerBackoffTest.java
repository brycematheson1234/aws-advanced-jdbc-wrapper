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

package software.amazon.jdbc.hostlistprovider.monitoring;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class NodeMonitoringWorkerBackoffTest {

  @Test
  void testCalculateBackoffWithJitter_exponentialGrowth() throws Exception {
    long attempt0 = invokeCalculateBackoff(0);
    assertTrue(attempt0 >= 50 && attempt0 <= 100, "Attempt 0 should be 50-100ms, was: " + attempt0);

    long attempt1 = invokeCalculateBackoff(1);
    assertTrue(attempt1 >= 100 && attempt1 <= 200, "Attempt 1 should be 100-200ms, was: " + attempt1);

    long attempt2 = invokeCalculateBackoff(2);
    assertTrue(attempt2 >= 200 && attempt2 <= 400, "Attempt 2 should be 200-400ms, was: " + attempt2);

    long attempt3 = invokeCalculateBackoff(3);
    assertTrue(attempt3 >= 400 && attempt3 <= 800, "Attempt 3 should be 400-800ms, was: " + attempt3);
  }

  @Test
  void testCalculateBackoffWithJitter_cappedAtMax() throws Exception {
    long highAttempt = invokeCalculateBackoff(100);
    assertTrue(highAttempt >= 2500 && highAttempt <= 5000,
        "High attempt should be capped at 2500-5000ms, was: " + highAttempt);
  }

  @Test
  void testCalculateBackoffWithJitter_jitterVariance() throws Exception {
    boolean hasVariance = false;
    long firstValue = invokeCalculateBackoff(5);
    for (int i = 0; i < 10; i++) {
      long value = invokeCalculateBackoff(5);
      if (value != firstValue) {
        hasVariance = true;
        break;
      }
    }
    assertTrue(hasVariance, "Jitter should produce variance across multiple calls");
  }

  private long invokeCalculateBackoff(int attempt) throws Exception {
    Class<?>[] innerClasses = ClusterTopologyMonitorImpl.class.getDeclaredClasses();
    for (Class<?> innerClass : innerClasses) {
      if (innerClass.getSimpleName().equals("NodeMonitoringWorker")) {
        Method method = innerClass.getDeclaredMethod("calculateBackoffWithJitter", int.class);
        method.setAccessible(true);
        return (long) method.invoke(null, attempt);
      }
    }
    throw new IllegalStateException("NodeMonitoringWorker class not found");
  }
}
