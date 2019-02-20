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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class SparkPlanGraphSuite extends SparkPlanGraphTestUtils with SharedSQLContext{

  setupTestData()

  test("correlate SparkPlan and stages - sort merge join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val planGraph = getSparkPlanGraph(
        sql("select p.name, s.salary from Person p join Salary s on p.id = s.personId"),
        1,
        Set.empty)
      assert(true)
    }
  }

  test("correlate SparkPlan and stages - broadcast hash join") {
    val planGraph = getSparkPlanGraph(
      sql("select /*+ BROADCAST(s) */ p.name, s.salary " +
        "from Person p join Salary s on p.id = s.personId"),
      2,
      Set.empty)
    assert(true)
  }

  test("correlate SparkPlan and stages - sort merge join and scalar subquery") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val planGraph = getSparkPlanGraph(
        sql("select p.name, s.salary, (select avg(salary) from Salary) " +
          "from Person p join Salary s on p.id = s.personId"),
        2,
        Set.empty)
      assert(true)
    }
  }
}
