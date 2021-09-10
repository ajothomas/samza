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

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;


/**
 * A Histogram is a {@link org.apache.samza.metrics.Metric} that keeps a thread-safe reference of
 * Dropwizard's {@link com.codahale.metrics.Histogram} to keep a track of a distribution of values. It uses
 * a reservoir which is statistically representative of the dataset to compute the distribution. This is called
 * Reservoir sampling. Please checkout various implementations of {@link Reservoir}, the default used here is a
 * {@link UniformReservoir} with a reservoir size of 1024.
 *
 * User can use {@link Builder} to build the histogram and can optionally pass custom list of percentiles
 *
 */
public class Histogram implements Metric {
  private static final Map<String, Function<Snapshot, Object>> DEFAULT_METRIC_EXTRACTOR_MAP =
      ImmutableMap.<String, Function<Snapshot, Object>>builder()
          .put("Min", Snapshot::getMin)
          .put("Max", Snapshot::getMax)
          .put("Mean", Snapshot::getMean)
          .put("StdDev", Snapshot::getStdDev)
          .put("P50", Snapshot::getMedian)
          .put("P75", Snapshot::get75thPercentile)
          .put("P95", Snapshot::get95thPercentile)
          .put("P98", Snapshot::get98thPercentile)
          .put("P99", Snapshot::get99thPercentile)
          .put("P99_9", Snapshot::get999thPercentile)
          .build();

  private String name;
  private AtomicReference<com.codahale.metrics.Histogram> histogram;
  private Map<String, Function<Snapshot, Object>> metricsExtractorMap;

  private Histogram() {
  }

  private Histogram(String name, Reservoir reservoir, List<Double> percentiles, boolean skipDefaultMetrics) {
    this.name = name;
    this.histogram = new AtomicReference<>(new com.codahale.metrics.Histogram(reservoir));
    this.metricsExtractorMap = createMetricsExtractorMap(percentiles, skipDefaultMetrics);
  }

  public void update(long value) {
    histogram.get().update(value);
  }

  public Map<String, Object> getMetrics() {
    final Snapshot snapshot = histogram.get().getSnapshot();
    return metricsExtractorMap.entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, kv -> {
          final Function<Snapshot, Object> metricExtractor = kv.getValue();
          return metricExtractor.apply(snapshot);
        }));
  }

  public String getName() {
    return name;
  }

  @VisibleForTesting
  Set<String> getDefaultMetricNames() {
    return DEFAULT_METRIC_EXTRACTOR_MAP.keySet();
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.histogram(this);
  }

  public Object getMetric(Function<Snapshot, Object> extractor) {
    return extractor.apply(histogram.get().getSnapshot());
  }

  /**
   * Creates a Map of metric names and its metric extractor. Metric Extractor is responsible for extracting the
   * metric from Histogram's {@link Snapshot}.
   * */
  @VisibleForTesting
  public Map<String, Function<Snapshot, Object>> createMetricsExtractorMap(List<Double> percentiles, boolean skipDefaultMetrics) {
    // create a map of Metric name and metric extractor from user supplied percentiles
    final ImmutableMap<String, Function<Snapshot, Object>> percentileExtractorMap = percentiles.stream()
        .filter(percentile -> percentile >= 0.0 && percentile <= 100.0)
        .collect(ImmutableMap.toImmutableMap(percentile -> "P" + getStringRepresentationForDouble(percentile),
            PercentileExtractor::new));

    if (!skipDefaultMetrics) {
      final ImmutableMap<String, Function<Snapshot, Object>> dedupedPercentileExtractorMap = percentileExtractorMap
          .entrySet()
          .stream()
          .filter(kv -> !DEFAULT_METRIC_EXTRACTOR_MAP.containsKey(kv.getKey()))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

      return ImmutableMap.<String, Function<Snapshot, Object>>builder()
          .putAll(DEFAULT_METRIC_EXTRACTOR_MAP)
          .putAll(dedupedPercentileExtractorMap)
          .build();
    } else {
      return percentileExtractorMap;
    }
  }

  private String getStringRepresentationForDouble(Double value) {
    String strValue;
    if (value == value.intValue()) {
      strValue = String.valueOf(value.intValue());
    } else {
      strValue = String.valueOf(value).replace(".", "_");
    }
    return strValue;
  }

  // Function to extract a percentile value from the Histogram snapshot
  private static class PercentileExtractor implements Function<Snapshot, Object> {
    private final Double percentile;

    PercentileExtractor(Double percentile) {
      this.percentile = percentile;
    }

    @Override
    public Object apply(Snapshot snapshot) {
      return snapshot.getValue(percentile / 100);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  // Builder class for Histogram
  public static class Builder {
    private static final Reservoir DEFAULT_RESERVOIR = new UniformReservoir();

    private String name;
    private Reservoir reservoir;
    private List<Double> percentiles;
    private boolean skipDefaultMetrics;

    private Builder() {
    }

    /**
     * Set the name of the metric
     * */
    public Builder setName(@Nullable String name) {
      this.name = name;
      return this;
    }

    /**
     * Set a reservoir for the histogram. The default is {@link UniformReservoir} with a reservoir size of 1024.
     * */
    public Builder setReservoir(@Nullable Reservoir reservoir) {
      this.reservoir = reservoir;
      return this;
    }

    /**
     * Set custom percentiles to report.
     * */
    public Builder setPercentiles(@Nullable List<Double> percentiles) {
      this.percentiles = percentiles;
      return this;
    }

    /**
     * The default metrics reported are defined in the map {@link #DEFAULT_METRIC_EXTRACTOR_MAP}. If we do not want to
     * report it, set it to true. Setting this to true would require the user to pass custom percentiles using
     * {@link #setPercentiles(List)}
     * */
    public Builder setSkipDefaultMetrics(boolean skipDefaultMetrics) {
      this.skipDefaultMetrics = skipDefaultMetrics;
      return this;
    }

    public Histogram build() {
      Preconditions.checkNotNull(this.name, "`name` field is required. Please use setName to set the field");
      // default reservoir = uniform reservoir
      if (this.reservoir == null) {
        this.reservoir = DEFAULT_RESERVOIR;
      }
      if (this.percentiles == null) {
        this.percentiles = new ArrayList<>();
      }
      if (this.skipDefaultMetrics && this.percentiles.size() == 0) {
        throw new RuntimeException("Histogram requires that a non-empty list percentiles should be defined "
            + "using .setPercentiles if skipDefaultMetrics is set to true");
      }

      return new Histogram(this.name, this.reservoir, this.percentiles, this.skipDefaultMetrics);
    }
  }
}
