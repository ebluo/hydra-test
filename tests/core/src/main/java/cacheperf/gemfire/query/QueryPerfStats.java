/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package cacheperf.gemfire.query;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import hydra.ClientPrms;
import hydra.HydraRuntimeException;
import hydra.HydraVector;
import hydra.Log;
import hydra.TestConfig;
import cacheperf.CachePerfStats;

import org.apache.geode.*;
import org.apache.geode.cache.query.Query;
import org.apache.geode.internal.NanoTimer;

import distcache.DistCache;

import perffmwk.HistogramStats;
import perffmwk.PerfReportPrms;
import perffmwk.PerformanceStatistics;
import util.TestException;

/**
 * Implements statistics related to query performance tests.
 */
public class QueryPerfStats extends CachePerfStats {
 
  //////////////////////// Static Methods ////////////////////////

  /**
   * Returns the statistic descriptors for <code>QueryPerfStats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    HydraVector hvQueries = TestConfig.tab()
    .vecAt(QueryPerfPrms.query, null);
    Object obj = TestConfig.tab().get(
        PerfReportPrms.useAutoGeneratedStatisticsSpecification);
    
    boolean individualQueryStats = (obj == null? false: Boolean.valueOf((String)obj));
    if (individualQueryStats) {
      String statsSpecFile = System.getProperty("user.dir") + "/"
          + "autoGenerate.spec";
      PrintWriter pw = null;
      try {
        pw = new PrintWriter(new FileWriter(new File(statsSpecFile)));
        pw.write("include $JTESTS/smoketest/perf/common.spec;\n\n");
        
        pw.write("statspec queriesPerSecond * cacheperf.gemfire.query.QueryPerfStats * queries\n");
        pw.write("filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries;\n");
        
        pw.write("\n");
        
        pw.write("statspec totalQueryExecutions * cacheperf.gemfire.query.QueryPerfStats * queries\n");
        pw.write("filter=none combine=combineAcrossArchives ops=max-min? trimspec=queries;\n");
        
        pw.write("\n");
        
        pw.write("statspec totalQueryExecutionTimeMillis * cacheperf.gemfire.query.QueryPerfStats * queryTime\n");
        pw.write("filter=none combine=combineAcrossArchives ops=max-min? trimspec=queries;\n");
        
        pw.write("\n");
        
        pw.write("expr overallQueryResponseTimeMillis = totalQueryExecutionTimeMillis / totalQueryExecutions ops=max-min?\n");
        pw.write(";\n");
        pw.write("expr overallQueryThroughPut = totalQueryExecutions / totalQueryExecutionTimeMillis ops=max-min?\n");
        pw.write(";\n");
        pw.write("\n");
        for (int i = 0; i < hvQueries.size(); i++) {
//          pw.write("statspec query" + (i + 1) + "PerSecond * cacheperf.gemfire.query.QueryPerfStats * query" + (i + 1) + " \n");
//          pw.write("filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries;\n");
          
          pw.write("\n");
          
          pw.write("statspec totalQuery" + (i < 9? "0":"")+(i + 1) + "Executions" + " * cacheperf.gemfire.query.QueryPerfStats * query" + (i + 1) + " \n");
          pw.write("filter=none combine=combineAcrossArchives ops=max-min? trimspec=queries;\n");
          
          pw.write("\n");
          
          pw.write("statspec totalQuery" + (i < 9? "0":"")+(i + 1) + "ExecutionTimeMillis * cacheperf.gemfire.query.QueryPerfStats * query" + (i + 1) + "Time \n");
          pw.write("filter=none combine=combineAcrossArchives ops=max-min? trimspec=queries;\n");
          
          pw.write("\n");
          
          pw.write("expr query" + (i < 9? "0":"") + (i + 1) + "ResponseTimeMillis = totalQuery" + (i < 9? "0":"")+(i + 1) + "ExecutionTimeMillis / totalQuery" + (i < 9? "0":"")+(i + 1) + "Executions ops=max-min?\n");
          pw.write(";\n");
          pw.write("expr query" + (i < 9? "0":"") + (i + 1) + "PerMilliSecond = totalQuery" + (i < 9? "0":"")+(i + 1) + "Executions / totalQuery" + (i < 9? "0":"")+(i + 1) + "ExecutionTimeMillis ops=max-min?\n");
          pw.write(";\n");
          pw.write("\n");
//          pw.write("statspec query" + (i + 1) + "Results"
//              + "PerSecond * cacheperf.gemfire.query.QueryPerfStats * query"
//              + (i + 1) + "Results \n");
//          pw.write("filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries;\n");
//
//          pw.write("statspec totalQuery" + (i + 1) + "Time"
//              + " * cacheperf.gemfire.query.QueryPerfStats * query" + (i + 1)
//              + "Time \n");
//          pw.write("filter=none combine=combineAcrossArchives ops=max-min? trimspec=queries;\n");
          
          
          
        }
        pw.close();
      }
      catch (IOException e) {
        String s = "Unable to open file: " + statsSpecFile;
        throw new TestException(s, e);
      }
    }
    
    StatisticDescriptor[] finalSd = null;

    StatisticDescriptor[] sd1 = CachePerfStats.getStatisticDescriptors();
    Log.getLogWriter().info("hvQueries: " + hvQueries);
    Log.getLogWriter().info("sd1: " + sd1);
    finalSd = sd1;
    if (individualQueryStats) {
      StatisticDescriptor[] sd = new StatisticDescriptor[sd1.length + 3
          * hvQueries.size()];
      System.arraycopy(sd1, 0, sd, 0, sd1.length);
      for (int i = 0; i < hvQueries.size(); i++) {
        sd[sd1.length + i * 3] = factory().createIntCounter("query" + (i + 1),
            "Number of queries completed.", "operations", largerIsBetter);
        sd[sd1.length + i * 3 + 1] = factory().createIntCounter(
            "query" + (i + 1) + "Results", "Size of query result set.",
            "entries", largerIsBetter);
        sd[sd1.length + i * 3 + 2] = factory().createLongCounter(
            "query" + (i + 1) + "Time", "Total time spent doing queries.",
            "nanoseconds", !largerIsBetter);
      }
      finalSd = sd;
    }
   
    return finalSd;
  }
  
  /**
   * increase the time on the optional histogram by the supplied amount
   */
  public void incHistogram(HistogramStats histogram, long amount) {
    if (histogram != null) {
      histogram.incBin(amount);
    }
  }

  /**
   * increase the count on the vmCount
   */
  private synchronized void incVMCount() {
    if (!VMCounted) {
      statistics().incInt(VM_COUNT, 1);
      VMCounted = true;
    }
  }
  public static QueryPerfStats getInstance() {
    QueryPerfStats qps = (QueryPerfStats)getInstance(QueryPerfStats.class,
        THREAD_SCOPE);
    Log.getLogWriter().info("QueryPerfStats getInstance qps Statistics class name: " + qps.getClass().getName());
    return qps;
  }
  private static boolean VMCounted = false;
  
  public static QueryPerfStats getInstance(int scope) {
    QueryPerfStats qps = (QueryPerfStats)getInstance(QueryPerfStats.class,
        scope);
    return qps;
  }

  public static QueryPerfStats getInstance(String name) {
    QueryPerfStats qps = (QueryPerfStats)getInstance(QueryPerfStats.class,
        THREAD_SCOPE, name);
    return qps;
  }

  public static QueryPerfStats getInstance(String name, String trimspecName) {
    QueryPerfStats qps = (QueryPerfStats)getInstance(QueryPerfStats.class,
        THREAD_SCOPE, name, trimspecName);
    return qps;
  }

  // ///////////////// Construction / initialization ////////////////

  public QueryPerfStats(Class cls, StatisticsType type, int scope,
      String instanceName, String trimspecName) {
    super(cls, type, scope, instanceName, trimspecName);
  }

  // ///////////////// Accessing stats ////////////////////////

  public int getQuery(int queryNum) {
    return statistics().getInt("query" + queryNum);
  }

  public int getQueryResults(int queryNum) {
    return statistics().getInt("query" + queryNum + QUERY_RESULTS);
  }

  public long getQueryTime(int queryNum) {
    return statistics().getLong("query" + queryNum + QUERY_TIME);
  }

  // ///////////////// Updating stats /////////////////////////

  /**
   * increase the count on the queries
   */
  public void incQueries(boolean isMainWorkload) {
    incQueries(1, isMainWorkload);
  }

  /**
   * increase the count on the queries by the supplied amount
   */
  public void incQueries(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(QUERIES, amount);
  }

  /**
   * increase the count on the query results by the supplied amount
   */
  public void incQueryResults(int amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt(QUERY_RESULTS, amount);
  }
  
  /**
   * increase the time on the queries by the supplied amount
   */
  public void incQueryTime(long amount, boolean isMainWorkload) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong(QUERY_TIME, amount);
  }
  
  /**
   * increase the count on the queries
   */
  public void incQueries(boolean isMainWorkload, int queryNum) {
    incQueries(1, isMainWorkload, queryNum);
  }

  /**
   * increase the count on the queries by the supplied amount
   */
  public void incQueries(int amount, boolean isMainWorkload, int queryNum) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt("query" + queryNum, amount);
  }

  /**
   * increase the count on the query results by the supplied amount
   */
  public void incQueryResults(int amount, boolean isMainWorkload, int queryNum) {
    if (isMainWorkload) {
      statistics().incInt(OPS, amount);
    }
    statistics().incInt("query" + queryNum + "Results", amount);
  }

  /**
   * increase the time on the queries by the supplied amount
   */
  public void incQueryTime(long amount, boolean isMainWorkload, int queryNum) {
    if (isMainWorkload) {
      statistics().incLong(OP_TIME, amount);
    }
    statistics().incLong("query" + queryNum + "Time", amount);
  }

  /**
   * @return the timestamp that marks the start of the query
   */
  public long startQuery() {
    return NanoTimer.getTime();
  }

  /**
   * @param start
   *          the timestamp taken when the query started
   */
  public void endQuery(long timeElapsed, int results, boolean isMainWorkload,
      HistogramStats histogram) {
    //long elapsed = NanoTimer.getTime() - start;
    incQueries(1, isMainWorkload);
    incQueryResults(results, isMainWorkload);
    incQueryTime(timeElapsed/(1000*1000), isMainWorkload);
    incHistogram(histogram, timeElapsed);
  }
  
  /**
   * @return the timestamp that marks the start of the query
   */
  public long startQuery(int queryNum) {
    return NanoTimer.getTime();
  }

  /**
   * @param start
   *          the timestamp taken when the query started
   */
  public void endQuery(long timeElapsed, int results, boolean isMainWorkload,
      HistogramStats histogram, int queryNum) {
    //long elapsed = NanoTimer.getTime() - start;
    endQuery(timeElapsed, results, isMainWorkload, histogram);
    incQueries(1, isMainWorkload, queryNum);
    incQueryResults(results, isMainWorkload, queryNum);
    incQueryTime(timeElapsed/(1000*1000), isMainWorkload, queryNum);
    incHistogram(histogram, timeElapsed);
  }
}
