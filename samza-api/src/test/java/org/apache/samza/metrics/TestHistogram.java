/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

/**
 * Tests for {@link Histogram} metric.
 * */
public class TestHistogram {
  private static final String METRIC_NAME = "Metric1";

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testNameFieldRequired() {
    exceptionRule.expect(NullPointerException.class);
    exceptionRule.expectMessage("`name` field is required. Please use setName to set the field");
    Histogram histogram = Histogram.builder().build();
  }

  @Test
  public void testDefaultMetricsMatchesReturnedMetrics() {
    Histogram histogram = Histogram.builder().setName(METRIC_NAME).build();
    Set<String> returnedMetrics = histogram.getMetrics().keySet();
    Set<String> defaultMetricNames = histogram.getDefaultMetricNames();
    assertEquals(returnedMetrics, defaultMetricNames);
  }

  @Test
  public void testSkipDefaultMetricsNeedsPercentilesSet() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Histogram requires that a non-empty list percentiles should be defined "
        + "using .setPercentiles if skipDefaultMetrics is set to true");
    Histogram histogram = Histogram.builder()
        .setName(METRIC_NAME)
        .setSkipDefaultMetrics(true)
        .build();
  }

  @Test
  public void testSkipDefaultMetricsDoesNotAllowEmptyPercentiles() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Histogram requires that a non-empty list percentiles should be defined "
        + "using .setPercentiles if skipDefaultMetrics is set to true");
    Histogram histogram = Histogram.builder()
        .setName(METRIC_NAME)
        .setSkipDefaultMetrics(true)
        .setPercentiles(new ArrayList<>())
        .build();
  }

  @Test
  public void testSkipDefaultMetrics() {
    Histogram histogram = Histogram.builder()
        .setName(METRIC_NAME)
        .setSkipDefaultMetrics(true)
        .setPercentiles(ImmutableList.of(0.0, 10.1, 11.0, 99.9)).build();

    Set<String> returnedMetrics = histogram.getMetrics().keySet();
    ImmutableSet<String> expectedMetrics = ImmutableSet.of("P0", "P10_1", "P11", "P99_9");
    assertEquals(returnedMetrics, expectedMetrics);
  }

  @Test
  public void testPercentiles() {
    Histogram histogram = Histogram.builder()
        .setName(METRIC_NAME)
        .setPercentiles(ImmutableList.of(0.0, 10.1, 11.0, 99.9)).build();

    Set<String> returnedMetrics = histogram.getMetrics().keySet();
    Set<String> expectedMetrics = Sets.union(
        histogram.getDefaultMetricNames(), ImmutableSet.of("P0", "P10_1", "P11", "P99_9")).immutableCopy();
    assertEquals(returnedMetrics, expectedMetrics);
  }

  @Test
  public void testInvalidPercentilesAreFiltered() {
    Histogram histogram = Histogram.builder()
        .setName(METRIC_NAME)
        .setSkipDefaultMetrics(true)
        .setPercentiles(ImmutableList.of(-1.0, 0.05, 10.1, 11.0, 99.99, 101.0)).build();

    Set<String> returnedMetrics = histogram.getMetrics().keySet();
    Set<String> expectedMetrics = ImmutableSet.of("P0_05", "P10_1", "P11", "P99_99");
    assertEquals(returnedMetrics, expectedMetrics);
  }

  @Test
  public void testGetMetrics() {
    Histogram histogram = Histogram.builder()
        .setName(METRIC_NAME)
        .setPercentiles(ImmutableList.of(0.01, 1.0, 60.0)).build();

    for (long i = 1L; i < 100L; ++i) {
      histogram.update(i);
    }
    Map<String, Object> metrics = histogram.getMetrics();
    assertEquals(metrics.get("Min"), 1L);
    assertEquals(metrics.get("Max"), 99L);
    assertEquals(metrics.get("Mean"), 50.0);
    assertEquals(metrics.get("P50"), 50.0);
    assertEquals(metrics.get("P75"), 75.0);
    assertEquals(metrics.get("P95"), 95.0);
    assertEquals(metrics.get("P98"), 98.0);
    assertEquals(metrics.get("P99"), 99.0);
    assertEquals(metrics.get("P99_9"), 99.0);
    // custom percentile check
    assertEquals(metrics.get("P0_01"), 1.0);
    assertEquals(metrics.get("P1"), 1.0);
    assertEquals(metrics.get("P60"), 60.0);
  }
}
