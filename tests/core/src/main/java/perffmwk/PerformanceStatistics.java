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

package perffmwk;

import org.apache.geode.*;
import hydra.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * An instance of a performance statistics type.  It uses the process id of the
 * process that creates it as its numeric display name, and registers itself
 * with the hydra master for reporting purposes.
 */
public class PerformanceStatistics {

  //////////////////////////////////////////////////////////////////////////////
  ////    STATIC FIELDS                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used to specify that an instance will be used by a single thread.
   */
  public static final int THREAD_SCOPE = 0;

  /**
   *  Used to specify that an instance will be used by all threads in a VM.
   */
  public static final int VM_SCOPE     = 1;

  /**
   *  The statistics factory.
   */
  private static StatisticsFactory Factory;

  /**
   *  Maps statistics type class names to the instance of that type.
   */
  private static Map AllTypes = new HashMap();

  /**
   *  Maps statistics type class names to instances of statistics of that type.
   */
  private static Map AllInstances = new HashMap();

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE FIELDS                                                   ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The statistics instance.
   */
  protected Statistics statistics;

  /**
   *  The name of the statistics instance.
   */
  String statisticsInstanceName;

  /**
   *  The class that created this instance.
   */
  private Class cls;

  /**
   *  The scope of this instance, {@link #THREAD_SCOPE} or {@link #VM_SCOPE}.
   */
  int scope;

  /**
   *  The name of the trim specification with which to associate this statistics
   *  instance for runtime-generated statistics specifications.
   */
  String trimspecName;

  //////////////////////////////////////////////////////////////////////////////
  ////    STATIC METHODS                                                    ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the factory used to create statistics descriptors and types.
   */
  public static synchronized StatisticsFactory factory() {
    if ( Factory == null ) {
      Factory = DistributedSystemHelper.getDistributedSystem();
      if (Factory == null) {
        Factory = DistributedSystemHelper.connect();
      }
    }
    return Factory;
  }

  /**
   * Gets the statistics instance with the specified class, an autogenerated
   * display name based on the specified scope, associated with the default
   * trim specification.  Lazily creates the instance.
   */
  public static PerformanceStatistics getInstance(Class cls, int scope) {
    return getInstance(cls, scope, null, null);
  }

  /**
   * Gets the statistics instance with the specified class and an autogenerated
   * display name based on the specified name and scope, associated with the
   * default trim specification.  Lazily creates the instance.
   */
  public static PerformanceStatistics getInstance(Class cls, int scope,
                                                  String name) {
    return getInstance(cls, scope, name, null);
  }

  /**
   * Gets the statistics instance with the specified class and an autogenerated
   * display name based on the specified scope and name, associated with the
   * specified trim specification name.  Lazily creates the type and instance.
   */
  public static PerformanceStatistics getInstance(Class cls, int scope,
                                      String name, String trimspecName) {

    StatisticsType type = getStatisticsType(cls, scope);
    String instanceName = instanceNameFor(scope, name);
    return getInstance(cls, type, scope, instanceName, trimspecName);
  }

  /**
   * Returns the StatisticsType for the class.  Lazily creates and caches the
   * type, invoking <code>getStatisticDescriptors</code> on the class to
   * generate the StatisticDescriptor array.
   */
  private static StatisticsType getStatisticsType(Class cls, int scope) {
    synchronized (AllTypes) {
      StatisticsType type = (StatisticsType)AllTypes.get(cls.getName());
      if (type == null) {
        Log.getLogWriter().info("Creating statistics type " + cls.getName());
        MethExecutorResult res = MethExecutor.execute
                                 (cls.getName(), "getStatisticDescriptors");
        if (res.exceptionOccurred()) {
          String msg = "Unable to get statistic descriptors for " + cls;
          throw new PerfStatException(msg, res.getException());
        }
        type = factory().createType(cls.getName(),
               "Application statistics with " + scopeNameFor(scope) + " scope",
               (StatisticDescriptor[])res.getResult());
        AllTypes.put(cls.getName(), type);
        AllInstances.put(cls.getName(), new HashMap());
        Log.getLogWriter().info("Created statistics type " + cls.getName());
      }
      return type;
    }
  }

  /**
   * Returns the StatisticsType for the class.  Lazily creates and caches the
   * instance, invoking the default constructor on the class.
   */
  private static PerformanceStatistics getInstance(Class cls,
    StatisticsType type, int scope, String instanceName, String trimspecName) {

    HashMap instanceMap = (HashMap)AllInstances.get(cls.getName());
    synchronized (instanceMap) {
      PerformanceStatistics instance =
         (PerformanceStatistics)instanceMap.get(instanceName);
      if (instance == null) {
        try {
          Log.getLogWriter().info("Creating statistics instance named "
                            + instanceName + " of type " + cls.getName());
          Constructor c = cls.getConstructor(
                            new Class[] {
                                          Class.class,
                                          StatisticsType.class,
                                          int.class,
                                          String.class,
                                          String.class
                                        });
          instance = (PerformanceStatistics)c.newInstance(
                            new Object[] {
                                           cls,
                                           type,
                                           new Integer(scope),
                                           instanceName,
                                           trimspecName
                                         });
          instanceMap.put(instanceName, instance);
          Log.getLogWriter().info("Created statistics instance named "
                            + instanceName + " of type " + cls.getName());

        } catch (Exception e) {
          String s = "Unable to create instance of " + cls;
          throw new PerfStatException(s, e);
        }
      }
      return instance;
    }
  }

  /**
   * Closes the statistics instance.
   */
  public void close() {
    // make sure that the statistics are sampled past the end trim 
    MasterController.sleepForMs(1000);

    HashMap instanceMap = (HashMap)AllInstances.get(this.getClass().getName());
    if (instanceMap != null) {
      synchronized (instanceMap) {
        PerformanceStatistics instance =
           (PerformanceStatistics)instanceMap.get(this.statisticsInstanceName);
        if (instance != null) {
          Log.getLogWriter().info("Closing statistics instance named "
                            + this.statisticsInstanceName + " of type "
                            + this.getClass().getName());
          instanceMap.remove(this.statisticsInstanceName);
          instance.statistics().close();
          Log.getLogWriter().info("Closed statistics instance named "
                            + this.statisticsInstanceName + " of type "
                            + this.getClass().getName());
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Creates an instance with the specified type, scope, name, and trim
   *  specification name.  The display name is generated based on the scope and
   *  name.  For per-thread scope, it is the name (defaults to "Thread") plus
   *  the logical hydra thread ID.  For per-VM scope, it is the name (defaults
   *  to "VM").  Valid values for scope are {@link #THREAD_SCOPE} and
   *  {@link #VM_SCOPE}.  The named trim specification is associated with the
   *  instance for reporting purposes (defaults to
   *  {@link TrimSpec#DEFAULT_TRIM_SPEC_NAME}).
   */
  public PerformanceStatistics(Class cls, StatisticsType type, int scope,
                               String instanceName, String trimspecName) {
    this.statisticsInstanceName = instanceName;
    this.cls = cls;
    this.scope = scope;
    this.trimspecName = trimspecName;
    if ( this.trimspecName == null ) {
      this.trimspecName = TrimSpec.DEFAULT_TRIM_SPEC_NAME;
    }
    long start = System.currentTimeMillis();
    switch (scope) {
      case THREAD_SCOPE:
        this.statistics = factory().createStatistics(type,
                                    this.statisticsInstanceName,
                                    ProcessMgr.getProcessId());
        break;
      case VM_SCOPE:
        this.statistics = factory().createAtomicStatistics(type,
                                    this.statisticsInstanceName,
                                    ProcessMgr.getProcessId(), 0);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized scope: " + scope);
    }
    if ( Log.getLogWriter().fineEnabled() ) {
      long t = System.currentTimeMillis() - start;
      Log.getLogWriter().fine( "Time to create stats "
                        + this.statisticsInstanceName + ", in ms: " + t );
    }
    start = System.currentTimeMillis();
    PerfStatMgr.getInstance().registerStatistics( this );
    if ( Log.getLogWriter().fineEnabled() ) {
      long t = System.currentTimeMillis() - start;
      Log.getLogWriter().fine( "Time to register stats "
                        + this.statisticsInstanceName + ", in ms: " + t );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE METHODS                                                  ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns the name of this instance.
   */
  public String getName() {
    return this.statisticsInstanceName;
  }

  /**
   *  Returns the statistics instance.
   */
  public Statistics statistics() {
    return this.statistics;
  }

  /**
   *  Returns the statistic descriptor for the statistic with the given name
   *  for this instance.
   *
   *  @throws PerfStatException if the instance has no stat name is not found.
   */
  public StatisticDescriptor getStatisticDescriptor( String statName ) {
    StatisticDescriptor[] sds = this.statistics.getType().getStatistics();
    for ( int i = 0; i < sds.length; i++ ) {
      String name = sds[i].getName();
      if ( name.equals( statName ) ) {
        return sds[i];
      }
    }
    throw new PerfStatException( "No statistic with name " + statName );
  }

  /**
   *  Returns the logical name of the trim specification for these statistics.
   */
  public String getTrimSpecName() {
    return this.trimspecName;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    INTERNALS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  private static String instanceNameFor( int scope, String name ) {
    switch( scope ) {
      case THREAD_SCOPE:
        int tid = RemoteTestModule.getCurrentThread().getThreadId();
        return ( name == null ) ? "Thread-" + tid : name + "-" + tid;
      case VM_SCOPE:
        int pid = ProcessMgr.getProcessId();
        return ( name == null ) ? "VM-" + pid : name + "-" + pid;
      default:
        throw new IllegalArgumentException( "Unrecognized scope: " + scope );
    }
  }

  private static String scopeNameFor( int scope ) {
    switch( scope ) {
      case THREAD_SCOPE:
        return "thread";
      case VM_SCOPE:
        return "VM";
      default:
        throw new IllegalArgumentException( "Unrecognized scope: " + scope );
    }
  }
}
