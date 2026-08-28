/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.cloud.run.crema;

import static java.lang.Math.clamp;
import static java.lang.Math.max;

import com.google.cloud.run.crema.clients.CloudRunClientWrapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.MetadataKey;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Performs scaling logic for a single ScaledObject.
 *
 * <p>This class takes a configuration of a ScaledObject and its metrics, applies stabilization
 * logic, and updates the number of consumer instances in Cloud Run.
 */
public class Scaler {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private static final MetadataKey<String> RESOURCE = MetadataKey.single("resource", String.class);
  private static final MetadataKey<Integer> CURRENT_INSTANCE_COUNT = MetadataKey.single("currentInstanceCount", Integer.class);
  private static final MetadataKey<Integer> RECOMMENDED_INSTANCE_COUNT = MetadataKey.single("recommendedInstanceCount", Integer.class);
  private static final MetadataKey<Integer> MIN_REPLICA_COUNT = MetadataKey.single("minReplicaCount", Integer.class);
  private static final MetadataKey<Integer> MAX_REPLICA_COUNT = MetadataKey.single("maxReplicaCount", Integer.class);

  private static final String RECOMMENDED_INSTANCE_COUNT_METRIC_NAME = "recommended_instance_count";
  private static final String REQUESTED_INSTANCE_COUNT_METRIC_NAME = "requested_instance_count";
  private static final String METRIC_VALUE_METRIC_NAME = "metric_value";
  private static final String TARGET_VALUE_METRIC_NAME = "target_value";
  private static final String TARGET_AVERAGE_VALUE_METRIC_NAME = "target_average_value";

  private final Map<String, ScalingStabilizer> scalingStabilizers = new HashMap<>();
  private final CloudRunClientWrapper cloudRunClientWrapper;
  private final MetricsService metricsService;
  private final ConfigurationProvider.StaticConfig staticConfig;
  private final String projectId;
  private final Clock clock;

  public Scaler(
      CloudRunClientWrapper cloudRunClientWrapper,
      MetricsService metricsService,
      ConfigurationProvider.StaticConfig config,
      String projectId) {
    this(cloudRunClientWrapper, metricsService, config, projectId, Clock.systemUTC());
  }

  Scaler(
      CloudRunClientWrapper cloudRunClientWrapper,
      MetricsService metricsService,
      ConfigurationProvider.StaticConfig config,
      String projectId,
      Clock clock) {
    this.cloudRunClientWrapper =
        Preconditions.checkNotNull(cloudRunClientWrapper, "Cloud Run client cannot be null.");
    this.metricsService =
        Preconditions.checkNotNull(metricsService, "Metrics service cannot be null.");
    this.staticConfig = Preconditions.checkNotNull(config, "Static config cannot be null.");
    this.projectId = Preconditions.checkNotNull(projectId, "Project ID cannot be null.");
    this.clock = Preconditions.checkNotNull(clock, "Clock cannot be null.");
  }

  /**
   * Scales the target Cloud Run service or worker pool based on the provided metrics.
   *
   * <p>A valid trigger is a metric with a non-zero target value. If no valid triggers are found,
   * the scaling for the given workload will be considered failed.
   *
   * @param scaledObjectMetrics The metrics for the scaled object.
   * @throws IOException If an error occurs while communicating with Cloud Run or other services.
   */
  public ScalingStatus scale(ScaledObjectMetrics scaledObjectMetrics)
      throws IOException, ExecutionException, InterruptedException {
    Instant now = clock.instant();

    String workloadName = scaledObjectMetrics.getScaledObject().getScaleTargetRef().getName();
    final WorkloadInfoParser.WorkloadInfo workloadInfo = WorkloadInfoParser.parse(workloadName);

    if (workloadInfo.workloadType() == WorkloadInfoParser.WorkloadType.WORKERPOOL
        && staticConfig.useMinInstances()) {
      throw new IllegalArgumentException(
          "USE_MIN_INSTANCES is not supported for worker pool workloads.");
    }

    boolean zeroOnlyGithubRunnerWorkerPool =
        staticConfig.githubRunnerZeroOnlyScaleDown()
            && workloadInfo.workloadType() == WorkloadInfoParser.WorkloadType.WORKERPOOL;

    if (zeroOnlyGithubRunnerWorkerPool
        && scaledObjectMetrics.getFailedTriggerTypesList().contains("github-runner")) {
      logger
          .atWarning()
          .with(RESOURCE, workloadName)
          .log(
              "GitHub runner metrics failed for %s; preserving the worker pool because zero-only"
                  + " scale-down is enabled.",
              workloadName);
      return ScalingStatus.FAILED;
    }

    int currentInstanceCount =
        InstanceCountProvider.getInstanceCount(cloudRunClientWrapper, workloadInfo);
    logger.atInfo().with(RESOURCE, workloadName).with(CURRENT_INSTANCE_COUNT, currentInstanceCount)
    .log("Current instances for %s: %d", workloadName, currentInstanceCount);

    int unboundedRecommendation = 0;
    boolean hasValidTrigger = false;
    boolean hasGithubRunnerMetric = false;
    boolean hasValidGithubRunnerTrigger = false;

    if (scaledObjectMetrics.getMetricsCount() == 0) {
      if (zeroOnlyGithubRunnerWorkerPool) {
        logger
            .atWarning()
            .with(RESOURCE, workloadName)
            .log(
                "No metrics configured for %s; preserving the worker pool because GitHub runner"
                    + " zero-only scale-down is enabled.",
                workloadName);
        return ScalingStatus.FAILED;
      }
      logger.atInfo().with(RESOURCE, workloadName)
      .log("No metrics configured for %s, scaling down to 0", workloadName);
      updateInstanceCount(0, workloadInfo);
      return ScalingStatus.SUCCEEDED;
    }

    for (Metric metric : scaledObjectMetrics.getMetricsList()) {
      boolean isGithubRunnerTrigger = metric.getTriggerType().equals("github-runner");
      hasGithubRunnerMetric |= isGithubRunnerTrigger;
      int recommendation;
      if (metric.hasTargetAverageValue() && metric.getTargetAverageValue() > 0) {
        recommendation =
            TargetAverageValueScaling.makeRecommendation(
                metric.getValue(), metric.getTargetAverageValue(), currentInstanceCount);
      } else if (metric.hasTargetValue() && metric.getTargetValue() > 0) {
        recommendation =
            TargetValueScaling.makeRecommendation(
                metric.getValue(), metric.getTargetValue(), currentInstanceCount);
      } else {
        logger.atWarning().log(
            "Target value and target average value for %s are 0. At least one must be a"
                + " non-zero value. Skipping scaling workload %s on the trigger.",
            metric.getTriggerId(), workloadName);
        continue;
      }
      logger.atInfo().log(
          "Recommendation for %s based on scaling trigger %s: %d",
          workloadName, metric.getTriggerId(), recommendation);

      if (staticConfig.outputScalerMetrics()) {
        emitTriggerMetrics(metric, workloadInfo);
      }

      unboundedRecommendation = max(unboundedRecommendation, recommendation);
      hasValidTrigger = true;
      hasValidGithubRunnerTrigger |= isGithubRunnerTrigger;
    }

    if (!hasValidTrigger) {
      logger.atWarning().log(
          "No valid triggers found for %s. Skipping scaling workload.", workloadName);
      return ScalingStatus.FAILED;
    }

    if (zeroOnlyGithubRunnerWorkerPool
        && hasGithubRunnerMetric
        && !hasValidGithubRunnerTrigger) {
      logger
          .atWarning()
          .with(RESOURCE, workloadName)
          .log(
              "No valid GitHub runner metric found for %s; preserving the worker pool because"
                  + " zero-only scale-down is enabled.",
              workloadName);
      return ScalingStatus.FAILED;
    }

    boolean useGithubRunnerZeroOnlyScaleDown =
        zeroOnlyGithubRunnerWorkerPool && hasValidGithubRunnerTrigger;
    int rawRecommendation = unboundedRecommendation;
    if (useGithubRunnerZeroOnlyScaleDown
        && unboundedRecommendation > 0
        && unboundedRecommendation < currentInstanceCount) {
      logger
          .atInfo()
          .with(RESOURCE, workloadName)
          .log(
              "Preserving %d instances for %s because GitHub runner zero-only scale-down is"
                  + " enabled; the recommendation was %d.",
              currentInstanceCount, workloadName, unboundedRecommendation);
      unboundedRecommendation = currentInstanceCount;
    }

    Advanced.ScalerConfig scalerConfig =
        scaledObjectMetrics.getScaledObject().getAdvanced().getScalerConfig();

    ScalingStabilizer scalingStabilizer =
        scalingStabilizers.computeIfAbsent(
            workloadInfo.name(),
            (String k) ->
                useGithubRunnerZeroOnlyScaleDown
                    ? new ScalingStabilizer(currentInstanceCount, now)
                    : new ScalingStabilizer(currentInstanceCount));

    if (useGithubRunnerZeroOnlyScaleDown
        && currentInstanceCount == 0
        && unboundedRecommendation > 0) {
      // Upstream's 0->1 fast path returns before recording a recommendation.
      // Seed it explicitly so a transient zero on the next cycle cannot drain
      // the runner before the scale-down stabilization window elapses.
      scalingStabilizer.recordRecommendation(now, unboundedRecommendation);
    }

    int newInstanceCount =
        getBoundedRecommendation(
            currentInstanceCount,
            unboundedRecommendation,
            scalingStabilizer,
            scalerConfig,
            now,
            workloadName);

    if (useGithubRunnerZeroOnlyScaleDown
        && rawRecommendation > 0
        && newInstanceCount < currentInstanceCount) {
      logger
          .atInfo()
          .with(RESOURCE, workloadName)
          .log(
              "Preserving %d instances for %s because bounding changed a nonzero GitHub runner"
                  + " recommendation from %d to %d.",
              currentInstanceCount, workloadName, rawRecommendation, newInstanceCount);
      newInstanceCount = currentInstanceCount;
    }

    logger.atInfo().with(RESOURCE, workloadName).with(RECOMMENDED_INSTANCE_COUNT, newInstanceCount)
    .log("Recommended instances for %s: %d", workloadName, newInstanceCount);
    if (newInstanceCount != currentInstanceCount) {
      updateInstanceCount(newInstanceCount, workloadInfo);
      scalingStabilizer.markScaleEvent(
          scalerConfig.getBehavior(), now, currentInstanceCount, newInstanceCount);
    } else {
      logger.atInfo().with(RESOURCE, workloadName)
      .log("Recommended instances for %s is unchanged.", workloadName);
    }

    if (staticConfig.outputScalerMetrics()) {
      emitInstanceCountMetrics(unboundedRecommendation, newInstanceCount, workloadInfo);
    }

    return ScalingStatus.SUCCEEDED;
  }

  // Output a recommendation according to stabilization and min and max instances
  private int getBoundedRecommendation(
      int currentInstanceCount,
      int unboundedRecommendation,
      ScalingStabilizer scalingStabilizer,
      Advanced.ScalerConfig scalerConfig,
      Instant now,
      String workloadName) {

    int stabilizedInstanceCount =
        scalingStabilizer.getStabilizedRecommendation(
            scalerConfig.getBehavior(),
            now,
            currentInstanceCount,
            unboundedRecommendation,
            workloadName);

    int newInstanceCount =
        clamp(
            stabilizedInstanceCount,
            scalerConfig.getMinInstances(),
            scalerConfig.getMaxInstances());

    if (newInstanceCount != stabilizedInstanceCount) {
      logger.atInfo()
          .with(RESOURCE, workloadName)
          .with(MIN_REPLICA_COUNT, scalerConfig.getMinInstances())
          .with(MAX_REPLICA_COUNT, scalerConfig.getMaxInstances())
          .log(
          "Recommendation for %s was clamped to range",
          workloadName);
    }

    return newInstanceCount;
  }

  private void updateInstanceCount(
      int newInstanceCount, WorkloadInfoParser.WorkloadInfo workloadInfo)
      throws ExecutionException, IOException, InterruptedException {
    if (staticConfig.useMinInstances()) {
      if (workloadInfo.workloadType() == WorkloadInfoParser.WorkloadType.SERVICE) {
        try {
          cloudRunClientWrapper.updateServiceMinInstances(
              workloadInfo.name(),
              newInstanceCount,
              workloadInfo.projectId(),
              workloadInfo.location());
        } catch (ExecutionException | InterruptedException | com.google.api.gax.rpc.ApiException e) {
          logger.atWarning().withCause(e).log(
              "Failed to update min instances for %s", workloadInfo.name());
          throw new IOException(e);
        }
      } else {
        // We should never realistically get here because we should have checked against this in
        // the constructor.
        throw new IllegalArgumentException("Min instances are not supported for worker pools.");
      }
    } else {
      if (workloadInfo.workloadType() == WorkloadInfoParser.WorkloadType.SERVICE) {
        try {
          cloudRunClientWrapper.updateServiceManualInstances(
              workloadInfo.name(),
              newInstanceCount,
              workloadInfo.projectId(),
              workloadInfo.location());
        } catch (UnsupportedOperationException | com.google.api.gax.rpc.ApiException e) {
          throw new IOException(e);
        }
        logger.atInfo().log(
            "Sent update request for service %s to set instances to %d.",
            workloadInfo.name(), newInstanceCount);
      } else {
        try {
          cloudRunClientWrapper.updateWorkerPoolManualInstances(
              workloadInfo.name(),
              newInstanceCount,
              workloadInfo.projectId(),
              workloadInfo.location());
        } catch (com.google.api.gax.rpc.ApiException e) {
          throw new IOException(e);
        }
        logger.atInfo().log(
            "Sent update request for workerpool %s to set instances to %d.",
            workloadInfo.name(), newInstanceCount);
      }
    }
  }

  private void emitInstanceCountMetrics(
      int recommendedInstanceCount,
      int newInstanceCount,
      WorkloadInfoParser.WorkloadInfo workloadInfo) {
    ImmutableMap<String, String> metricLabels =
        ImmutableMap.of("project_id", projectId, "consumer_service", workloadInfo.name());
    try {
      metricsService.writeMetricIgnoreFailure(
          RECOMMENDED_INSTANCE_COUNT_METRIC_NAME, (double) recommendedInstanceCount, metricLabels);
      metricsService.writeMetricIgnoreFailure(
          REQUESTED_INSTANCE_COUNT_METRIC_NAME, (double) newInstanceCount, metricLabels);
    } catch (RuntimeException ex) {
      // An exception here is not critical to scaling. Log the exception and continue.
      logger.atWarning().withCause(ex).log("Failed to write metrics to Cloud Monitoring.");
    }
  }

  private void emitTriggerMetrics(Metric metric, WorkloadInfoParser.WorkloadInfo workloadInfo) {
    ImmutableMap<String, String> metricLabels =
        ImmutableMap.of(
            "project_id",
            projectId,
            "consumer_service",
            workloadInfo.name(),
            "trigger_id",
            metric.getTriggerId());
    try {
      metricsService.writeMetricIgnoreFailure(
          metric.getTriggerType() + "/" + METRIC_VALUE_METRIC_NAME,
          metric.getValue(),
          metricLabels);

      // metric either hasTargetAverageValue or hasTargetValue because of the one-of proto
      // definition
      if (metric.hasTargetAverageValue()) {
        metricsService.writeMetricIgnoreFailure(
            metric.getTriggerType() + "/" + TARGET_AVERAGE_VALUE_METRIC_NAME,
            metric.getTargetAverageValue(),
            metricLabels);
      } else {
        metricsService.writeMetricIgnoreFailure(
            metric.getTriggerType() + "/" + TARGET_VALUE_METRIC_NAME,
            metric.getTargetValue(),
            metricLabels);
      }
    } catch (RuntimeException ex) {
      // An exception here is not critical to scaling. Log the exception and continue.
      logger.atWarning().withCause(ex).log("Failed to write metrics to Cloud Monitoring.");
    }
  }
}
