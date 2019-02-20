/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.ui

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.test.SQLTestUtils

trait SparkPlanGraphTestUtils extends SQLTestUtils {

  protected def statusStore: SQLAppStatusStore = spark.sharedState.statusStore

  protected def currentExecutionIds(): Set[Long] = {
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    statusStore.executionsList.map(_.executionId).toSet
  }

  protected def getSparkPlanGraph(df: DataFrame,
      expectedNumOfJobs: Int,
      expectedNodeIds: Set[Long]): Option[SparkPlanGraph] = {
    val previousExecutionIds = currentExecutionIds()
    df.collect()
    sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds = currentExecutionIds().diff(previousExecutionIds)
    assert(executionIds.size === 1)
    val executionId = executionIds.head
    val jobs = statusStore.execution(executionId).get.jobs
    // Use "<=" because there is a race condition that we may miss some jobs
    // TODO Change it to "=" once we fix the race condition that missing the JobStarted event.
    assert(jobs.size <= expectedNumOfJobs)
    if (jobs.size == expectedNumOfJobs) {
      Some(SparkPlanGraph(SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)))
    } else {
      // TODO Remove this "else" once we fix the race condition that missing the JobStarted event.
      // Since we cannot track all jobs, the metric values could be wrong and we should not check
      // them.
      logWarning("Due to a race condition, we miss some jobs and cannot verify the metric values")
      None
    }
  }

}
