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
package com.gemstone.gemfire.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import junit.framework.AssertionFailedError;
import junitExt.MultiThreadedTestRunner;
import junitExt.TestRunnable;
import objects.Order;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.internal.Endpoint;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.BridgeWriter;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.BridgeTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.cache30.TestCacheLoader;
import com.gemstone.gemfire.cache30.TestCacheWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PoolStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifierStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.concurrent.AB;
import com.gemstone.gemfire.internal.concurrent.CFactory;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * This class tests the bridge loader and hierarchical caching mechanism in GemFire.
 * It does so by creating a cache server with a cache and a pre-defined region and
 * a data loader. The client creates the same region and attaches the BridgeLoader
 * as its cache loader (this happens in the controller VM). the client then spins up
 * 10 different threads and issues gets on keys. The server data loader returns the
 * data to the client via the hierarchical cache mechanism (used by the BridgeLoader)
 * Test uses Groboutils TestRunnable objects to achieve multi threading behavior
 * in the test.
 *
 * @author Greg Passmore 
 * Based on the BridgeLoaderTest
 * 
 */
public class ConnectionPoolDUnitTest extends CacheTestCase {

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;

  protected static int port = 0;
  protected static int port2 = 0;

  protected static int numberOfAfterInvalidates;
  protected static int numberOfAfterCreates;
  protected static int numberOfAfterUpdates;

  protected final static int TYPE_CREATE = 0;
  protected final static int TYPE_UPDATE = 1;
  protected final static int TYPE_INVALIDATE = 2;
  protected final static int TYPE_DESTROY = 3;

  public ConnectionPoolDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating BridgeLoaders
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        Map pools = PoolManager.getAll();
        if (!pools.isEmpty()) {
          getLogWriter().warning("found pools remaining after teardown: " + pools);
          assertEquals(0, pools.size());
        }
      }
    });
  }

  protected/*GemStoneAddition*/ static PoolImpl getPool(Region r) {
    PoolImpl result = null;
    String poolName = r.getAttributes().getPoolName();
    if (poolName != null) {
      result = (PoolImpl)PoolManager.find(poolName);
    }
    return result;
  }
  protected static TestCacheWriter getTestWriter(Region r) {
    return (TestCacheWriter)r.getAttributes().getCacheWriter();
  }
  /**
   * Create a bridge server on the given port without starting it.
   *
   * @since 5.0.2
   */
  protected void createBridgeServer(int port) throws IOException {
    CacheServer bridge = getCache().addCacheServer();
    bridge.setPort(port);
    bridge.setMaxThreads(getMaxThreads());
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   *
   * @since 4.0
   */
  protected void startBridgeServer(int port)
    throws IOException {
    startBridgeServer(port, -1);
  }
  
  protected void startBridgeServer(int port, int socketBufferSize) throws IOException {
    startBridgeServer(port, socketBufferSize, CacheServer.DEFAULT_LOAD_POLL_INTERVAL);
  }

  protected void startBridgeServer(int port, int socketBufferSize, long loadPollInterval)
    throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    if (socketBufferSize != -1) {
      bridge.setSocketBufferSize(socketBufferSize);
    }
    bridge.setMaxThreads(getMaxThreads());
    bridge.setLoadPollInterval(loadPollInterval);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * By default return 0 which turns off selector and gives thread per cnx.
   * Test subclasses can override to run with selector.
   * @since 5.1
   */
  protected int getMaxThreads() {
    return 0;
  }

  /**
   * Stops the bridge server that serves up the given cache.
   *
   * @since 4.0
   */
  void stopBridgeServer(Cache cache) {
    CacheServer bridge =
        cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  void stopBridgeServers(Cache cache) {
    CacheServer bridge = null;
    for (Iterator bsI = cache.getCacheServers().iterator();bsI.hasNext(); ) {
      bridge = (CacheServer) bsI.next();
      bridge.stop();
      assertFalse(bridge.isRunning());
    }
  }

  private void restartBridgeServers(Cache cache) throws IOException
  {
    CacheServer bridge = null;
    for (Iterator bsI = cache.getCacheServers().iterator();bsI.hasNext(); ) {
      bridge = (CacheServer) bsI.next();
      bridge.start();
      assertTrue(bridge.isRunning());
    }
  }

  protected InternalDistributedSystem createLonerDS() {
    disconnectFromDS();
    InternalDistributedSystem ds = getLonerSystem();
    assertEquals(0, ds.getDistributionManager().getOtherDistributionManagerIds().size());
    return ds;
  }

  
  
  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false); // test validation expects this behavior
    return factory.create();
  }

  private static String createBridgeClientConnection(String host, int[] ports) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < ports.length; i++) {
      if (i > 0) sb.append(",");
      sb.append("name" + i + "=");
      sb.append(host + ":" + ports[i]);
    }
    return sb.toString();
  }

  private class EventWrapper {
    public final EntryEvent event;
    public final Object key;
    public final Object val;
    public final Object arg;
    public final  int type;
    public EventWrapper(EntryEvent ee, int type) {
      this.event = ee;
      this.key = ee.getKey();
      this.val = ee.getNewValue();
      this.arg = ee.getCallbackArgument();
      this.type = type;
    }
    @Override
    public String toString() {
      return "EventWrapper: event=" + event + ", type=" + type;
    }
  }

  protected class ControlListener extends CacheListenerAdapter {
    public final LinkedList events = new LinkedList();
    public final Object CONTROL_LOCK = new Object();

    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount) {
      long maxMillis = System.currentTimeMillis() + sleepMs;
      synchronized (this.CONTROL_LOCK) {
        try {
          while (this.events.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            this.CONTROL_LOCK.wait(waitMillis);
          }
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !this.events.isEmpty();
      }
    }

    @Override
    public void afterCreate(EntryEvent e) {
      //System.out.println("afterCreate: " + e);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_CREATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterUpdate(EntryEvent e) {
      //System.out.println("afterUpdate: " + e);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_UPDATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterInvalidate(EntryEvent e) {
      //System.out.println("afterInvalidate: " + e);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_INVALIDATE));
        this.CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterDestroy(EntryEvent e) {
      //System.out.println("afterDestroy: " + e);
      synchronized(this.CONTROL_LOCK) {
        this.events.add(new EventWrapper(e, TYPE_DESTROY));
        this.CONTROL_LOCK.notifyAll();
      }
    }
  }

  


  /**
   * Create a fake EntryEvent that returns the provided region for {@link CacheEvent#getRegion()}
   * and returns {@link com.gemstone.gemfire.cache.Operation#LOCAL_LOAD_CREATE} for {@link CacheEvent#getOperation()}
   * @param r
   * @return fake entry event
   */
  protected static EntryEvent createFakeyEntryEvent(final Region r) {
    return new EntryEvent() {
      public Operation getOperation()
      {
        return Operation.LOCAL_LOAD_CREATE; // fake out bridgewriter/bridgeclient to exit early
      }
      public Region getRegion()
      {
        return r;
      }
      public Object getKey() { return null; }
      public Object getOldValue() { return null;}
      public boolean isOldValueAvailable() {return true;}
      public Object getNewValue() { return null;}
      public boolean isLocalLoad() { return false;}
      public boolean isNetLoad() {return false;}
      public boolean isLoad() {return true; }
      public boolean isNetSearch() {return false;}
      public TransactionId getTransactionId() {return null;}
      public Object getCallbackArgument() {return null;}
      public boolean isCallbackArgumentAvailable() {return true;}
      public boolean isOriginRemote() {return false;}
      public DistributedMember getDistributedMember() {return null;}
      public boolean isExpiration() { return false;}
      public boolean isDistributed() { return false;}
      public boolean isBridgeEvent() {
        return hasClientOrigin();
      }
      public boolean hasClientOrigin() {
        return false;
      }
      public ClientProxyMembershipID getContext() {
        return null;
      }
      public SerializedCacheValue getSerializedOldValue() {return null;}
      public SerializedCacheValue getSerializedNewValue() {return null;}
    };
  }

  private void runGets(final String name, final String objectName) throws Throwable {
    int threadCount = 40;
    TestRunnable [] runnables = new TestRunnable[threadCount];
    for (int i=0;i<threadCount;i++) {
      String objectRequested = objectName + i;
      runnables[i] = new GetKey(objectRequested, this, name,10);
    }
    new MultiThreadedTestRunner(runnables).runTestRunnables();
  }

  private final AB timeToStop = CFactory.createAB(false);
  private Thread gettorThread;
  
  private void startRunningGets(final String name, final String objectName) throws Throwable {
    this.timeToStop.set(false);
    int threadCount = 10;
    final TestRunnable [] runnables = new TestRunnable[threadCount];
    for (int i=0;i<threadCount;i++) {
      String objectRequested = objectName + i;
      runnables[i] = new GetKey(objectRequested, this, name, this.timeToStop);
    }
    this.gettorThread = new Thread(new Runnable() {
      public void run() {
        try {
          new MultiThreadedTestRunner(runnables).runTestRunnables();
        } catch (Throwable ex) {
          fail("unexpected exception", ex);
        }
      }});
    this.gettorThread.start();
  }
  private void stopRunningGets() throws InterruptedException {
    timeToStop.set(true);
    try {
      DistributedTestCase.join(this.gettorThread, 30 * 1000, getLogWriter());
    } finally {
      this.gettorThread = null;
    }
  }

  public void testBridgeLoaderFailover()
  throws CacheException, InterruptedException, Throwable {

    // Initialize values
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "A";
//    final Integer value = new Integer(440);

    //Create the same region in 3 VMs
    SerializableRunnable create =
      new CacheSerializableRunnable("Create ACK Region on Server") {
      @Override
      public void run2() {
        try {

          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              //System.err.println("CacheServer data loader called");
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);

          Region region = createRegion(name, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name));
          startBridgeServer(0, -1, 100);
        }
        catch(Exception e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer");
        }
      }
    };

    vm0.invoke(create);
    port = vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");

    vm1.invoke(create);
    port2 = vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");


    SerializableRunnable killCacheServer =
      new SerializableRunnable("Kill CacheServer") {
          public void run() {
            stopBridgeServer(getCache());
          }
        };

    final int[] restartPort = new int[1];

    SerializableRunnable reStartCacheServer =
      new SerializableRunnable("Restart CacheServer") {
      public void run() {
        try {
          Region region = getRootRegion().getSubregion(name);
          assertNotNull(region);
          startBridgeServer(restartPort[0], -1, 100);
        }
        catch(Exception e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer", e);
        }

      }
    };

    //Start a test to get N keys in the BridgeLoader

    try {
      getLonerSystem();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      BridgeTestCase.configureConnectionPool(factory,getServerHostName(host),port,port2,false,-1,0, null, 900, 500, true, 500);
      createRegion(name, factory.create());
      assertNotNull(getRootRegion().getSubregion(name));

      //region.create(objectName, null);
    }
    catch (CacheException ex) {
      fail("While creating LOCAL region on Edge", ex);
    }
    PoolImpl pool = getPool(getRootRegion().getSubregion(name));
    //pool.releaseThreadLocalConnection();

    verifyServerCount(pool, 0);
    
    final String expected = "java.net.ConnectException";
    final String addExpected =
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    final String expected2 = "java.io.IOException";
    final String addExpected2 =
      "<ExpectedException action=add>" + expected2 + "</ExpectedException>";
    final String removeExpected2 =
      "<ExpectedException action=remove>" + expected2 + "</ExpectedException>";

    getCache().getLogger().info(addExpected);
    getCache().getLogger().info(addExpected2);


    try { // make sure we removeExpected

      runGets(name, objectName);
      verifyServerCount(pool, 2);

      //Kill one of the VMs
      vm0.invoke(killCacheServer);

      verifyServerCount(pool, 0); // wait for all cnxs to timeout

      // Run the getters
      runGets(name, objectName);
      verifyServerCount(pool, 1);

    } finally {
      getCache().getLogger().info(removeExpected);
      getCache().getLogger().info(removeExpected2);
    }

    restartPort[0] = port;
    vm0.invoke(reStartCacheServer);


    int port3 = vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    assertTrue( port3 == port);

    verifyServerCount(pool, 0); // wait for all cnx to timeout

    waitForBlacklistToClear(pool);
    
    //Wait for locator to be notified of client departures
    //Thread.sleep(1000);

    //Verify that all gets succeed. Implicit, as the test would fail otherwise
    //Verify that connections to both CacheServers exist at the end of the test
//     assertTrue(loader.getActiveServers().size() == 2);
//    assertTrue(loader.getDeadServers().size() == 0);
  //  vm1.invoke(killCacheServer);
    startRunningGets(name, objectName);
    try {
      verifyBalanced(pool, 2, 5);
      Thread.sleep(1000);
      verifyBalanced(pool, 2, 5);
      Thread.sleep(1000);
      verifyBalanced(pool, 2, 5);
    } finally {
      stopRunningGets();
    }
 //   restartPort[0] = (int) port2;
 //   vm1.invoke(reStartCacheServer2);
 //   pause(3000);
 //   assertTrue(loader.getActiveServers().size() == 2);

    getRootRegion().getSubregion(name).localDestroyRegion();

    vm0.invoke(killCacheServer);
    vm1.invoke(killCacheServer);
  }

  public void verifyBalanced(final PoolImpl pool, int expectedServer, 
      final int expectedConsPerServer) {
    verifyServerCount(pool, expectedServer);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return balanced(pool, expectedConsPerServer);
      }
      public String description() {
        return "expected " + expectedConsPerServer
            + " but endpoints=" + outOfBalanceReport(pool);
      }
    };
    DistributedTestCase.waitForCriterion(ev, 2 * 60 * 1000, 200, true);
    assertEquals("expected " + expectedConsPerServer
                 + " but endpoints=" + outOfBalanceReport(pool),
                 true, balanced(pool, expectedConsPerServer));
  }
  protected boolean balanced(PoolImpl pool, int expectedConsPerServer) {
    Iterator it = pool.getEndpointMap().values().iterator();
    while (it.hasNext()) {
      Endpoint ep = (Endpoint)it.next();
      if (ep.getStats().getConnections() != expectedConsPerServer) {
        return false;
      }
    }
    return true;
  }
  
  protected String outOfBalanceReport(PoolImpl pool) {
    StringBuffer result = new StringBuffer();
    Iterator it = pool.getEndpointMap().values().iterator();
    result.append("<");
    while (it.hasNext()) {
      Endpoint ep = (Endpoint)it.next();
      result.append("ep=" + ep);
      result.append(" conCount=" + ep.getStats().getConnections());
      if (it.hasNext()) {
        result.append(", ");
      }
    }
    result.append(">");
    return result.toString();
  }
  
  public void waitForBlacklistToClear(final PoolImpl pool) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return pool.getBlacklistedServers().size() == 0;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
    assertEquals("unexpected blacklistedServers=" + pool.getBlacklistedServers(),
                 0, pool.getBlacklistedServers().size());
  }
  
  public void verifyServerCount(final PoolImpl pool, final int expectedCount) {
    getCache().getLogger().info("verifyServerCount expects=" + expectedCount);
    WaitCriterion ev = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int actual = pool.getConnectedServerCount();
        if (actual == expectedCount) {
          return true;
        }
        excuse = "Found only " + actual + " servers, expected " + expectedCount;
        return false;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 60 * 1000, 200, true);
  }

  /**
   * Tests that the callback argument is sent to the server
   */
  public void testCallbackArg() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final Object createCallbackArg = "CREATE CALLBACK ARG";
    final Object updateCallbackArg = "PUT CALLBACK ARG";

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {

          CacheWriter cw = new TestCacheWriter() {
              @Override
              public final void beforeUpdate2(EntryEvent event)
                throws CacheWriterException {
                Object beca = event.getCallbackArgument();
                assertEquals(updateCallbackArg, beca);
              }

              @Override
              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException {
                Object beca =  event.getCallbackArgument();
                assertEquals(createCallbackArg, beca);
              }
            };
            AttributesFactory factory = getBridgeServerRegionAttributes(null, cw);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    final int port2 =
      vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
//    final String host2 = getServerHostName(vm1.getHost());
    
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);

            System.out.println("!!!!!!!!!!!!!!!!! Host:"+host0+" port:"+port+" port2:"+port2);
            BridgeTestCase.configureConnectionPool(factory,getServerHostName(host),port,port2,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm1.invoke(new CacheSerializableRunnable("Add entries") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.create(new Integer(i), "old" + i, createCallbackArg);
          }
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "new" + i, updateCallbackArg);
        }
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Check cache writer") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          TestCacheWriter writer = getTestWriter(region);
          assertTrue(writer.wasInvoked());
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
    };

    vm1.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });

  }

  /**
   * Tests that consecutive puts have the callback assigned
   * appropriately.
   */
  public void testCallbackArg2() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final Object createCallbackArg = "CREATE CALLBACK ARG";
//    final Object updateCallbackArg = "PUT CALLBACK ARG";

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          CacheWriter cw = new TestCacheWriter() {
              @Override
              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException {
                Integer key = (Integer) event.getKey();
                if (key.intValue() % 2 == 0) {
                  Object beca =  event.getCallbackArgument();
                  assertEquals(createCallbackArg, beca);
                } else {
                  Object beca =  event.getCallbackArgument();
                  assertNull(beca);
                }
              }
            };
            AttributesFactory factory = getBridgeServerRegionAttributes(null, cw);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    final int port2 =
      vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
//    final String host2 = getServerHostName(vm1.getHost());
    
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,port2,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm1.invoke(new CacheSerializableRunnable("Add entries") {
        @Override
        public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
              region.create(new Integer(i), "old" + i, createCallbackArg);

            } else {
              region.create(new Integer(i), "old" + i);
            }
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);

    vm0.invoke(new CacheSerializableRunnable("Check cache writer") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          TestCacheWriter writer = getTestWriter(region);
          assertTrue(writer.wasInvoked());
        }
      });

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  
  /**
   * Tests for bug 36684 by having two bridge servers with cacheloaders that should always return
   * a value and one client connected to each server reading values. If the bug exists, the
   * clients will get null sometimes. 
   * @throws InterruptedException 
   */
  public void testBug36684() throws CacheException, InterruptedException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Create the cache servers with distributed, mirrored region
    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerMirroredAckRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);
    vm1.invoke(createServer);

    // Create cache server clients
    final int numberOfKeys = 1000;
    final String host0 = getServerHostName(host);
    final int vm0Port = vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final int vm1Port = vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    SerializableRunnable createClient =
      new CacheSerializableRunnable("Create Cache Server Client") {
        @Override
        public void run2() throws CacheException {
          // reset all static listener variables in case this is being rerun in a subclass
          numberOfAfterInvalidates = 0;
          numberOfAfterCreates  = 0;
          numberOfAfterUpdates = 0;
          // create the region
          getLonerSystem();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false); // test validation expects this behavior
          // create bridge writer
          BridgeTestCase.configureConnectionPool(factory,host0,vm0Port,vm1Port,true,-1,-1, null);
         createRegion(name, factory.create());
        }
      };
    getSystem().getLogWriter().info("before create client");
    vm2.invoke(createClient);
    vm3.invoke(createClient);

    // Initialize each client with entries (so that afterInvalidate is called)
    SerializableRunnable initializeClient =
      new CacheSerializableRunnable("Initialize Client") {
      @Override
      public void run2() throws CacheException {
//        StringBuffer errors = new StringBuffer();
          numberOfAfterInvalidates = 0;
          numberOfAfterCreates = 0;
          numberOfAfterUpdates = 0;
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          for (int i=0; i<numberOfKeys; i++) {
            String expected = "key-"+i;
            String actual = (String) region.get("key-"+i);
            assertEquals(expected, actual);
          }
      }
    };
    
    getSystem().getLogWriter().info("before initialize client");
    AsyncInvocation inv2 = vm2.invokeAsync(initializeClient);
    AsyncInvocation inv3 = vm3.invokeAsync(initializeClient);
    
    DistributedTestCase.join(inv2, 30 * 1000, getLogWriter());
    DistributedTestCase.join(inv3, 30 * 1000, getLogWriter());
    
    if (inv2.exceptionOccurred()) { 
      fail("Error occured in vm2", inv2.getException());
    }
    if(inv3.exceptionOccurred()) {
      fail("Error occured in vm3", inv3.getException());
    }
  }
  

  /**
   * Test for client connection loss with CacheLoader Exception on the server.
   */
  public void testForCacheLoaderException() throws CacheException, InterruptedException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    // Create the cache servers with distributed, mirrored region
    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              System.out.println("### CALLING CACHE LOADER....");
              throw new CacheLoaderException("Test for CahceLoaderException causing Client connection to disconnect.");
            }
            public void close() {
            }
          };
          AttributesFactory factory = getBridgeServerMirroredAckRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      };
    getSystem().getLogWriter().info("before create server");

    server.invoke(createServer);

    // Create cache server clients
    final int numberOfKeys = 10;
    final String host0 = getServerHostName(host);
    final int[] port = new int[] {server.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort")};
    final String poolName = "myPool";

    SerializableRunnable createClient =
      new CacheSerializableRunnable("Create Cache Server Client") {
        @Override
        public void run2() throws CacheException {
          getLonerSystem();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          // create bridge writer
          BridgeTestCase.configureConnectionPoolWithName(factory,host0,port,true,-1, -1, null, poolName);
         createRegion(name, factory.create());
        }
      };
    getSystem().getLogWriter().info("before create client");
    client.invoke(createClient);

    // Initialize each client with entries (so that afterInvalidate is called)
    SerializableRunnable invokeServerCacheLaoder =
      new CacheSerializableRunnable("Initialize Client") {
      @Override
      public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          PoolStats stats = ((PoolImpl)PoolManager.find(poolName)).getStats();
          int oldConnects = stats.getConnects();
          int oldDisConnects = stats.getDisConnects();
          try {
          for (int i=0; i<numberOfKeys; i++) {
            region.get("key-"+i);
          }
          } catch (Exception ex){
            if (!(ex.getCause() instanceof CacheLoaderException)) {
              fail ("UnExpected Exception, expected to receive CacheLoaderException from server, instead found: " + ex.getCause().getClass());
            }
          }
          int newConnects = stats.getConnects();
          int newDisConnects = stats.getDisConnects();
          //System.out.println("#### new connects/disconnects :" + newConnects + ":" + newDisConnects);
          if (newConnects != oldConnects && newDisConnects != oldDisConnects) {
            fail ("New connection has created for Server side CacheLoaderException.");
          }
      }
    };

    getSystem().getLogWriter().info("before initialize client");
    AsyncInvocation inv2 = client.invokeAsync(invokeServerCacheLaoder);
    
    DistributedTestCase.join(inv2, 30 * 1000, getLogWriter());
    SerializableRunnable stopServer = new SerializableRunnable("stop CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    };
    server.invoke(stopServer);
    
  }

  public void testBridgeLoader()
  throws CacheException, InterruptedException, Throwable {

    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);

    final String name = this.getUniqueName() + "-ACK";
    final String objectName = "A";
//    final Integer value = new Integer(440);
    String objectRequested = objectName;

    SerializableRunnable killCacheServer = new SerializableRunnable("Kill CacheServer") {
      public void run() {
        stopBridgeServer(getCache());
      }
    };

    server0.invoke(new SerializableRunnable("Create ACK Region on Server") {
      public void run() {
        try {

          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              //System.err.println("CacheServer data loader called");
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);

          Region region = createRegion(name, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name));
          //pause(1000);
          startBridgeServer(0);
        }
        catch (CacheException ex) {
          fail("Failed creating ACK Region on Server", ex);
        }
        catch(Exception e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer on VM1");
        }
      }
    });

    port = server0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");

    server1.invoke(new SerializableRunnable("Create ACK Region on Server") {
      public void run() {
        try {

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          factory.setCacheLoader(new CacheLoader() {
            public Object load(LoaderHelper helper) {
              //System.err.println("CacheServer data loader called");
              return helper.getKey();
            }
            public void close() {

            }
          });

          Region region = createRegion(name, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name));
          //pause(1000);
          startBridgeServer(0);
        }
        catch (CacheException ex) {
          fail("Failed creating ACK Region on Server", ex);
        }
        catch(Exception e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer on VM1");
        }
      }
    });

    port2 = server1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");

    Region region = null;
    try {
      getLonerSystem();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setConcurrencyChecksEnabled(false);
      BridgeTestCase.configureConnectionPool(factory, getServerHostName(host),port,port2,false,-1,-1, null);
      region = createRegion(name, factory.create());
      assertNotNull(getRootRegion().getSubregion(name));
      region.create(objectName, null);
    }
    catch (CacheException ex) {
      fail("While creating ACK region on Edge", ex);
    }

    int threadCount = 40;
    TestRunnable [] runnables = new TestRunnable[threadCount];
    for (int i=0;i<threadCount;i++) {
      objectRequested = objectName + i;
      runnables[i] = new GetKey(objectRequested,this, name,10);
    }
    MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(runnables);
    mttr.runTestRunnables();

    region.localDestroyRegion();

    server0.invoke(killCacheServer);
    server1.invoke(killCacheServer);
  }

  protected void validateDS() {
    List l = InternalDistributedSystem.getExistingSystems();
    if (l.size() > 1) {
      getSystem().getLogWriter().info("validateDS: size="
                                      + l.size()
                                      + " isDedicatedAdminVM="
                                      + DistributionManager.isDedicatedAdminVM
                                      + " l=" + l);
    }
    assertFalse(DistributionManager.isDedicatedAdminVM);
    assertEquals(1, l.size());
  }
  

  /**
   * Tests the basic operations of the {@link BridgeWriter}
   *
   * @since 3.5
   */
  public void testBridgeWriter() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          factory.setCacheLoader(new CacheLoader() {
              public Object load(LoaderHelper helper) {
                //System.err.println("CacheServer data loader called");
                return helper.getKey().toString();
              }
              public void close() {

              }
            });
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            validateDS();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };
    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Get values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get(new Integer(i));
            assertEquals(String.valueOf(i), value);
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Update values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);

          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), new Integer(i));
          }
        }
      });

    vm2.invoke(create);
    vm2.invoke(new CacheSerializableRunnable("Validate values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get(new Integer(i));
            assertNotNull(value);
            assertTrue(value instanceof Integer);
            assertEquals(i, ((Integer) value).intValue());
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Close BridgeWriter") {
        // do some special close validation here
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          String pName = region.getAttributes().getPoolName();
          PoolImpl p = (PoolImpl)PoolManager.find(pName);
          assertEquals(false, p.isDestroyed());
          assertEquals(1, p.getAttachCount());
          try {
            p.destroy();
            fail("expected IllegalStateException");
          } catch (IllegalStateException expected) {
          }
          region.localDestroyRegion();
          assertEquals(false, p.isDestroyed());
          assertEquals(0, p.getAttachCount());
        }
      });

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }
  
  
    
  
  /**
   * Tests the BridgeServer failover (bug 31832).
   */
  public void testBridgeServerFailoverCnx1() throws CacheException {
    disconnectAllFromDS();
    basicTestBridgeServerFailover(1);
  }
  /**
   * Test BridgeServer failover with connectionsPerServer set to 0
   */
  public void testBridgeServerFailoverCnx0() throws CacheException {
    basicTestBridgeServerFailover(0);
  }
  private void basicTestBridgeServerFailover(final int cnxCount) throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    // Create two bridge servers
    SerializableRunnable createCacheServer =
      new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
        }

        }
      };

    vm0.invoke(createCacheServer);
    vm1.invoke(createCacheServer);

    final int port0 =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    final int port1 =
      vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
//    final String host1 = getServerHostName(vm1.getHost());

    // Create one bridge client in this VM
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
        @Override
        public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port0,port1,true,-1,cnxCount, null, 100);

            Region region = createRegion(name, factory.create());

            // force connections to form
            region.put("keyInit", new Integer(0));
            region.put("keyInit2", new Integer(0));
        }
        };

    vm2.invoke(create);

    // Launch async thread that puts objects into cache. This thread will execute until
    // the test has ended (which is why the RegionDestroyedException and CacheClosedException
    // are caught and ignored. If any other exception occurs, the test will fail. See
    // the putAI.exceptionOccurred() assertion below.
    vm2.invokeAsync(new CacheSerializableRunnable("Put objects") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          try {
            for (int i=0; i<100000; i++) {
              region.put("keyAI", new Integer(i));
              try {Thread.sleep(100);} catch (InterruptedException ie) {
                fail("interrupted");
              }
            }
          } catch (NoAvailableServersException ignore) {
            /*ignore*/
          } catch (RegionDestroyedException e) { //will be thrown when the test ends
            /*ignore*/
          } 
          catch (CancelException e) { //will be thrown when the test ends
            /*ignore*/
          }
        }
      });


    SerializableRunnable verify1Server =
      new CacheSerializableRunnable("verify1Server") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          PoolImpl pool = getPool(region);
          verifyServerCount(pool, 1);
        }
        };
    SerializableRunnable verify2Servers =
      new CacheSerializableRunnable("verify2Servers") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          PoolImpl pool = getPool(region);
          verifyServerCount(pool, 2);
        }
      };

    vm2.invoke(verify2Servers);

    SerializableRunnable stopCacheServer =
      new SerializableRunnable("Stop CacheServer") {
          public void run() {
            stopBridgeServer(getCache());
          }
      };

    final String expected = "java.io.IOException";
    final String addExpected =
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected =
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    vm2.invoke(new SerializableRunnable() {
      public void run() {
        LogWriter bgexecLogger =
              new LocalLogWriter(LogWriterImpl.ALL_LEVEL, System.out);
        bgexecLogger.info(addExpected);
      }
    });
    try { // make sure we removeExpected

    // Bounce the non-current server (I know that VM1 contains the non-current server
    // because ...
    vm1.invoke(stopCacheServer);

    vm2.invoke(verify1Server);

    final int restartPort = port1;
    vm1.invoke(
      new SerializableRunnable("Restart CacheServer") {
      public void run() {
        try {
          Region region = getRootRegion().getSubregion(name);
          assertNotNull(region);
          startBridgeServer(restartPort);
        }
        catch(Exception e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer", e);
        }
      }
    });

    // Pause long enough for the monitor to realize the server has been bounced
    // and reconnect to it.
    vm2.invoke(verify2Servers);

    } finally {
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          LogWriter bgexecLogger =
                new LocalLogWriter(LogWriterImpl.ALL_LEVEL, System.out);
          bgexecLogger.info(removeExpected);
        }
      });
    }

    // Stop the other cache server
    vm0.invoke(stopCacheServer);

    // Run awhile
    vm2.invoke(verify1Server);

    getLogWriter().info("FIXME: this thread does not terminate"); // FIXME
//    // Verify that no exception has occurred in the putter thread
//    DistributedTestCase.join(putAI, 5 * 60 * 1000, getLogWriter());
//    //assertTrue("Exception occurred while invoking " + putAI, !putAI.exceptionOccurred());
//    if (putAI.exceptionOccurred()) {
//      fail("While putting entries: ", putAI.getException());
//    }

    // Close BridgeWriter
    vm2.invoke(new CacheSerializableRunnable("Close BridgeWriter") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      });

    // Stop the last cache server
    vm1.invoke(stopCacheServer);
  }
  

  /**
   * Make sure cnx lifetime expiration working on thread local cnxs.
   * @author darrel
   */
  public void testLifetimeExpireOnTL() throws CacheException {
    basicTestLifetimeExpire(true);
  }

  /**
   * Make sure cnx lifetime expiration working on thread local cnxs.
   * @author darrel
   */
  public void testLifetimeExpireOnPoolCnx() throws CacheException {
    basicTestLifetimeExpire(false);
  }

  protected static volatile boolean stopTestLifetimeExpire = false;

  protected static volatile int baselineLifetimeCheck;
  protected static volatile int baselineLifetimeExtensions;
  protected static volatile int baselineLifetimeConnect;
  protected static volatile int baselineLifetimeDisconnect;

  private void basicTestLifetimeExpire(final boolean threadLocal) throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    AsyncInvocation putAI = null;
    AsyncInvocation putAI2 = null;

    try {

      // Create two bridge servers
      SerializableRunnable createCacheServer =
        new CacheSerializableRunnable("Create Cache Server") {
          @Override
          public void run2() throws CacheException {
            AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
            factory.setCacheListener(new DelayListener(25));
            createRegion(name, factory.create());
            try {
              startBridgeServer(0);

            } catch (Exception ex) {
              fail("While starting CacheServer", ex);
            }

          }
        };

      vm0.invoke(createCacheServer);

      final int port0 =
        vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
      final String host0 = getServerHostName(vm0.getHost());
      vm1.invoke(createCacheServer);
      final int port1 =
        vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
      SerializableRunnable stopCacheServer =
        new SerializableRunnable("Stop CacheServer") {
          public void run() {
            stopBridgeServer(getCache());
          }
        };
      // we only had to stop it to reserve a port
      vm1.invoke(stopCacheServer);


      // Create one bridge client in this VM
      SerializableRunnable create =
        new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port0,port1,false/*queue*/,-1,0, null, 100, 500, threadLocal, 500);

            Region region = createRegion(name, factory.create());

            // force connections to form
            region.put("keyInit", new Integer(0));
            region.put("keyInit2", new Integer(0));
          }
        };

      vm2.invoke(create);

      // Launch async thread that puts objects into cache. This thread will execute until
      // the test has ended.
      SerializableRunnable putter1 = 
        new CacheSerializableRunnable("Put objects") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            PoolImpl pool = getPool(region);
            PoolStats stats = pool.getStats();
            baselineLifetimeCheck = stats.getLoadConditioningCheck();
            baselineLifetimeExtensions = stats.getLoadConditioningExtensions();
            baselineLifetimeConnect = stats.getLoadConditioningConnect();
            baselineLifetimeDisconnect = stats.getLoadConditioningDisconnect();
            try {
              int count = 0;
              while (!stopTestLifetimeExpire) {
                count++;
                region.put("keyAI1", new Integer(count));
              }
            } catch (NoAvailableServersException ex) {
              if (stopTestLifetimeExpire) {
                return;
              } else {
                throw ex;
              }
              //           } catch (RegionDestroyedException e) { //will be thrown when the test ends
              //             /*ignore*/
              //           } catch (CancelException e) { //will be thrown when the test ends
              //             /*ignore*/
            }
          }
        };
      SerializableRunnable putter2 = 
        new CacheSerializableRunnable("Put objects") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            try {
              int count = 0;
              while (!stopTestLifetimeExpire) {
                count++;
                region.put("keyAI2", new Integer(count));
              }
            } catch (NoAvailableServersException ex) {
              if (stopTestLifetimeExpire) {
                return;
              } else {
                throw ex;
              }
              //           } catch (RegionDestroyedException e) { //will be thrown when the test ends
              //             /*ignore*/
              //           } catch (CancelException e) { //will be thrown when the test ends
              //             /*ignore*/
            }
          }
        };
      putAI = vm2.invokeAsync(putter1);
      putAI2 = vm2.invokeAsync(putter2);

      SerializableRunnable verify1Server =
        new CacheSerializableRunnable("verify1Server") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            PoolImpl pool = getPool(region);
            final PoolStats stats = pool.getStats();
            verifyServerCount(pool, 1);
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                return stats.getLoadConditioningCheck() >= (10 + baselineLifetimeCheck);
              }
              public String description() {
                return null;
              }
            };
            DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
            
            // make sure no replacements are happening.
            // since we have 2 threads and 2 cnxs and 1 server
            // when lifetimes are up we should only want to connect back to the
            // server we are already connected to and thus just extend our lifetime
            assertTrue("baselineLifetimeCheck=" + baselineLifetimeCheck
                       + " but stats.getLoadConditioningCheck()=" + stats.getLoadConditioningCheck(),
                       stats.getLoadConditioningCheck() >= (10+baselineLifetimeCheck));
            baselineLifetimeCheck = stats.getLoadConditioningCheck();
            assertTrue(stats.getLoadConditioningExtensions() > baselineLifetimeExtensions);
            assertTrue(stats.getLoadConditioningConnect() == baselineLifetimeConnect);
            assertTrue(stats.getLoadConditioningDisconnect() == baselineLifetimeDisconnect);
          }
        };
      SerializableRunnable verify2Servers =
        new CacheSerializableRunnable("verify2Servers") {
          @Override
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            PoolImpl pool = getPool(region);
            final PoolStats stats = pool.getStats();
            verifyServerCount(pool, 2);
            // make sure some replacements are happening.
            // since we have 2 threads and 2 cnxs and 2 servers
            // when lifetimes are up we should connect to the other server sometimes.
//            int retry = 300;
//            while ((retry-- > 0)
//                   && (stats.getLoadConditioningCheck() < (10+baselineLifetimeCheck))) {
//              pause(100);
//            }
//            assertTrue("Bug 39209 expected "
//                       + stats.getLoadConditioningCheck()
//                       + " to be >= "
//                       + (10+baselineLifetimeCheck),
//                       stats.getLoadConditioningCheck() >= (10+baselineLifetimeCheck));
            
            // TODO: does this WaitCriterion actually help?
            WaitCriterion wc = new WaitCriterion() {
              String excuse;
              public boolean done() {
                int actual = stats.getLoadConditioningCheck();
                int expected = 10 + baselineLifetimeCheck;
                if (actual >= expected) {
                  return true;
                }
                excuse = "Bug 39209 expected " + actual + " to be >= " + expected;
                return false;
              }
              public String description() {
                return excuse;
              }
            };
            try {
              DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
            } catch (AssertionFailedError e) {
//              dumpStack();
              throw e;
            }
            
            assertTrue(stats.getLoadConditioningConnect() > baselineLifetimeConnect);
            assertTrue(stats.getLoadConditioningDisconnect() > baselineLifetimeDisconnect);
          }
        };

      vm2.invoke(verify1Server);
      assertEquals(true, putAI.isAlive());
      assertEquals(true, putAI2.isAlive());

      {
        final int restartPort = port1;
        vm1.invoke(new SerializableRunnable("Restart CacheServer") {
            public void run() {
              try {
                Region region = getRootRegion().getSubregion(name);
                assertNotNull(region);
                startBridgeServer(restartPort);
              }
              catch(Exception e) {
                getSystem().getLogWriter().fine(new Exception(e));
                fail("Failed to start CacheServer", e);
              }
            }
          });
      }

      vm2.invoke(verify2Servers);
      assertEquals(true, putAI.isAlive());
      assertEquals(true, putAI2.isAlive());
    } finally {
      vm2.invoke(new SerializableRunnable("Stop Putters") {
          public void run() {
            stopTestLifetimeExpire = true;
          }
        });

      try {
        if (putAI != null) {
          // Verify that no exception has occurred in the putter thread
          DistributedTestCase.join(putAI, 30 * 1000, getLogWriter());
          if (putAI.exceptionOccurred()) {
            fail("While putting entries: ", putAI.getException());
          }
        }
        
        if (putAI2 != null) {
          // Verify that no exception has occurred in the putter thread
          DistributedTestCase.join(putAI, 30 * 1000, getLogWriter());
          // FIXME this thread does not terminate
//          if (putAI2.exceptionOccurred()) {
//            fail("While putting entries: ", putAI.getException());
//          }
        }

      } finally {
        vm2.invoke(new SerializableRunnable("Stop Putters") {
            public void run() {
              stopTestLifetimeExpire = false;
            }
          });
        // Close BridgeWriter
        vm2.invoke(new CacheSerializableRunnable("Close BridgeWriter") {
            @Override
            public void run2() throws CacheException {
              Region region = getRootRegion().getSubregion(name);
              String poolName = region.getAttributes().getPoolName();
              region.localDestroyRegion();
              PoolManager.find(poolName).destroy();
            }
          });

        SerializableRunnable stopCacheServer =
          new SerializableRunnable("Stop CacheServer") {
            public void run() {
              stopBridgeServer(getCache());
            }
          };
        vm1.invoke(stopCacheServer);
        vm0.invoke(stopCacheServer);
      }
    }
  }

  /**
   * Tests the create operation of the {@link BridgeWriter}
   *
   * @since 3.5
   */
  public void testBridgeWriterCreate() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,false,-1,-1, null);
           createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm1.invoke(new CacheSerializableRunnable("Create values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.create(new Integer(i), new Integer(i));
          }
        }
      });

    vm2.invoke(create);
    vm2.invoke(new CacheSerializableRunnable("Validate values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get(new Integer(i));
            assertNotNull(value);
            assertTrue(value instanceof Integer);
            assertEquals(i, ((Integer) value).intValue());
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  /**
   * Tests the put operation of the {@link BridgeWriter}
   *
   * @since 3.5
   */
  public void testBridgeWriterPut() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable createBridgeWriter =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,false,-1,-1, null);
           createRegion(name, factory.create());
          }
        };

    vm1.invoke(createBridgeWriter);

    vm1.invoke(new CacheSerializableRunnable("Put values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            // put string values
            region.put("key-string-"+i, "value-"+i);

            // put object values
            Order order = new Order();
            order.init(i);
            region.put("key-object-"+i, order);

            // put byte[] values
            region.put("key-bytes-"+i, ("value-"+i).getBytes());
          }
        }
      });

    vm2.invoke(createBridgeWriter);

    vm2.invoke(new CacheSerializableRunnable("Get / validate string values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get("key-string-"+i);
            assertNotNull(value);
            assertTrue(value instanceof String);
            assertEquals("value-"+i, value);
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Get / validate object values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get("key-object-"+i);
            assertNotNull(value);
            assertTrue(value instanceof Order);
            assertEquals(i, ((Order) value).getIndex());
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Get / validate byte[] values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get("key-bytes-"+i);
            assertNotNull(value);
            assertTrue(value instanceof byte[]);
            assertEquals("value-"+i, new String((byte[]) value));
          }
        }
      });

  SerializableRunnable closeBridgeWriter =
    new CacheSerializableRunnable("Close BridgeWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(closeBridgeWriter);
    vm2.invoke(closeBridgeWriter);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }
    /**
   * Tests the put operation of the {@link BridgeWriter}
   *
   * @since 3.5
   */
  public void testBridgeWriterPutNoDeserialize() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null,null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable createBridgeWriter =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,false,-1,-1, null);
           createRegion(name, factory.create());
          }
        };

    vm1.invoke(createBridgeWriter);

    vm1.invoke(new CacheSerializableRunnable("Put values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            // put string values
            region.put("key-string-"+i, "value-"+i);

            // put object values
            Order order = new Order();
            order.init(i);
            region.put("key-object-"+i, order);

            // put byte[] values
            region.put("key-bytes-"+i, ("value-"+i).getBytes());
          }
        }
      });

    vm2.invoke(createBridgeWriter);

    vm2.invoke(new CacheSerializableRunnable("Get / validate string values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get("key-string-"+i);
            assertNotNull(value);
            assertTrue(value instanceof String);
            assertEquals("value-"+i, value);
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Get / validate object values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get("key-object-"+i);
            assertNotNull(value);
            assertTrue(value instanceof Order);
            assertEquals(i, ((Order) value).getIndex());
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Get / validate byte[] values") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object value = region.get("key-bytes-"+i);
            assertNotNull(value);
            assertTrue(value instanceof byte[]);
            assertEquals("value-"+i, new String((byte[]) value));
          }
        }
      });

  SerializableRunnable closeBridgeWriter =
    new CacheSerializableRunnable("Close BridgeWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(closeBridgeWriter);
    vm2.invoke(closeBridgeWriter);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
    pause(5 * 1000);
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link BridgeWriter}s.
   *
   * @since 3.5
   */
  public void testInvalidateAndDestroyPropagation() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        @Override
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }

      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          @Override
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            CertifiableTestCacheListener l = new CertifiableTestCacheListener(getLogWriter());
            factory.setCacheListener(l);
            Region rgn = createRegion(name, factory.create());
            rgn.registerInterestRegex(".*", false, false);
          }
        };

    vm1.invoke(create);
    vm1.invoke(new CacheSerializableRunnable("Populate region") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "old" + i);
          }
        }
      });
    vm2.invoke(create);
    pause(5 * 1000);
    
    vm1.invoke(new CacheSerializableRunnable("Turn on history") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          ctl.enableEventHistory();
        }
      });
    vm2.invoke(new CacheSerializableRunnable("Update region") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "new" + i, "callbackArg" + i);
          }
        }
      });
    pause(5 * 1000);

    vm1.invoke(new CacheSerializableRunnable("Verify invalidates") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            ctl.waitForInvalidated(key);
            Region.Entry entry = region.getEntry(key);
            assertNotNull(entry);
            assertNull(entry.getValue());
          }
          {
            List l = ctl.getEventHistory();
            assertEquals(10, l.size());
            for (int i = 0; i < 10; i++) {
              Object key = new Integer(i);
              EntryEvent ee = (EntryEvent)l.get(i);
              assertEquals(key, ee.getKey());
              assertEquals("old" + i, ee.getOldValue());
              assertEquals(Operation.INVALIDATE, ee.getOperation());
              assertEquals("callbackArg" + i, ee.getCallbackArgument());
              assertEquals(true, ee.isOriginRemote());
            }
          }
        }
      });


    vm2.invoke(new CacheSerializableRunnable("Validate original and destroy") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            assertEquals("new" + i, region.getEntry(key).getValue());
            region.destroy(key, "destroyCB"+i);
          }
        }
      });
    pause(5 * 1000);

    vm1.invoke(new CacheSerializableRunnable("Verify destroys") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            ctl.waitForDestroyed(key);
            Region.Entry entry = region.getEntry(key);
            assertNull(entry);
          }
          {
            List l = ctl.getEventHistory();
            assertEquals(10, l.size());
            for (int i = 0; i < 10; i++) {
              Object key = new Integer(i);
              EntryEvent ee = (EntryEvent)l.get(i);
              assertEquals(key, ee.getKey());
              assertEquals(null, ee.getOldValue());
              assertEquals(Operation.DESTROY, ee.getOperation());
              assertEquals("destroyCB"+i, ee.getCallbackArgument());
              assertEquals(true, ee.isOriginRemote());
            }
          }
        }
      });
    vm2.invoke(new CacheSerializableRunnable("recreate") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            region.create(key, "create" + i);
          }
        }
      });
    pause(5 * 1000);
    
    vm1.invoke(new CacheSerializableRunnable("Verify creates") {
        @Override
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          List l = ctl.getEventHistory();
          getLogWriter().info("history (should be empty): " + l);
          assertEquals(0, l.size());
          // now see if we can get it from the server
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            assertEquals("create"+i, region.get(key, "loadCB"+i));
          }
          l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            EntryEvent ee = (EntryEvent)l.get(i);
            getLogWriter().info("processing " + ee);
            assertEquals(key, ee.getKey());
            assertEquals(null, ee.getOldValue());
            assertEquals("create"+i, ee.getNewValue());
            assertEquals(Operation.LOCAL_LOAD_CREATE, ee.getOperation());
            assertEquals("loadCB"+i, ee.getCallbackArgument());
            assertEquals(false, ee.isOriginRemote());
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }
  /**
   * Tests that invalidates and destroys are propagated to {@link BridgeWriter}s
   * correctly to DataPolicy.EMPTY + InterestPolicy.ALL
   *
   * @since 5.0
   */
  public void testInvalidateAndDestroyToEmptyAllPropagation() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
       vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable createEmpty =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            CertifiableTestCacheListener l = new CertifiableTestCacheListener(getLogWriter());
            factory.setCacheListener(l);
            factory.setDataPolicy(DataPolicy.EMPTY);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            Region rgn = createRegion(name, factory.create());
            rgn.registerInterestRegex(".*", false, false);
          }
        };
    SerializableRunnable createNormal =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            CertifiableTestCacheListener l = new CertifiableTestCacheListener(getLogWriter());
            factory.setCacheListener(l);
            Region rgn = createRegion(name, factory.create());
            rgn.registerInterestRegex(".*", false, false);
          }
        };

    vm1.invoke(createEmpty);
    vm1.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "old" + i);
          }
        }
      });

    vm2.invoke(createNormal);
    vm1.invoke(new CacheSerializableRunnable("Turn on history") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          ctl.enableEventHistory();
        }
      });
    vm2.invoke(new CacheSerializableRunnable("Update region") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "new" + i, "callbackArg" + i);
      }
        }
    });
    pause(5 * 1000);

    vm1.invoke(new CacheSerializableRunnable("Verify invalidates") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            ctl.waitForInvalidated(key);
            Region.Entry entry = region.getEntry(key);
            assertNull(entry); // we are empty!
                }
          {
            List l = ctl.getEventHistory();
            assertEquals(10, l.size());
            for (int i = 0; i < 10; i++) {
              Object key = new Integer(i);
              EntryEvent ee = (EntryEvent)l.get(i);
              assertEquals(key, ee.getKey());
              assertEquals(null, ee.getOldValue());
              assertEquals(false, ee.isOldValueAvailable()); // failure
              assertEquals(Operation.INVALIDATE, ee.getOperation());
              assertEquals("callbackArg" + i, ee.getCallbackArgument());
              assertEquals(true, ee.isOriginRemote());
              }
          }

        }
      });


    vm2.invoke(new CacheSerializableRunnable("Validate original and destroy") {
          public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            assertEquals("new" + i, region.getEntry(key).getValue());
            region.destroy(key, "destroyCB"+i);
          }
        }
      });
    pause(5 * 1000);

    vm1.invoke(new CacheSerializableRunnable("Verify destroys") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            ctl.waitForDestroyed(key);
            Region.Entry entry = region.getEntry(key);
            assertNull(entry);
          }
          {
            List l = ctl.getEventHistory();
            assertEquals(10, l.size());
            for (int i = 0; i < 10; i++) {
              Object key = new Integer(i);
              EntryEvent ee = (EntryEvent)l.get(i);
              assertEquals(key, ee.getKey());
              assertEquals(null, ee.getOldValue());
              assertEquals(false, ee.isOldValueAvailable());
              assertEquals(Operation.DESTROY, ee.getOperation());
              assertEquals("destroyCB"+i, ee.getCallbackArgument());
              assertEquals(true, ee.isOriginRemote());
            }
          }
        }
      });
    vm2.invoke(new CacheSerializableRunnable("recreate") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            region.create(key, "create" + i, "createCB"+i);
          }
        }
      });
    pause(5 * 1000);
    
    vm1.invoke(new CacheSerializableRunnable("Verify creates") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            ctl.waitForInvalidated(key);
            Region.Entry entry = region.getEntry(key);
            assertNull(entry);
          }
          List l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            EntryEvent ee = (EntryEvent)l.get(i);
            assertEquals(key, ee.getKey());
            assertEquals(null, ee.getOldValue());
            assertEquals(false, ee.isOldValueAvailable());
            assertEquals(Operation.INVALIDATE, ee.getOperation());
            assertEquals("createCB"+i, ee.getCallbackArgument());
            assertEquals(true, ee.isOriginRemote());
          }
          // now see if we can get it from the server
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            assertEquals("create"+i, region.get(key, "loadCB"+i));
          }
          l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            EntryEvent ee = (EntryEvent)l.get(i);
            assertEquals(key, ee.getKey());
            assertEquals(null, ee.getOldValue());
            assertEquals("create"+i, ee.getNewValue());
            assertEquals(Operation.LOCAL_LOAD_CREATE, ee.getOperation());
            assertEquals("loadCB"+i, ee.getCallbackArgument());
            assertEquals(false, ee.isOriginRemote());
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  /**
   * Tests that invalidates and destroys are propagated to {@link BridgeWriter}s
   * correctly to DataPolicy.EMPTY + InterestPolicy.CACHE_CONTENT
   *
   * @since 5.0
   */
  public void testInvalidateAndDestroyToEmptyCCPropagation() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name, factory.create());
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable createEmpty =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            CertifiableTestCacheListener l = new CertifiableTestCacheListener(getLogWriter());
            factory.setCacheListener(l);
            factory.setDataPolicy(DataPolicy.EMPTY);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
            Region rgn = createRegion(name, factory.create());
            rgn.registerInterestRegex(".*", false, false);
         }
        };
    SerializableRunnable createNormal =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            CertifiableTestCacheListener l = new CertifiableTestCacheListener(getLogWriter());
            factory.setCacheListener(l);
            Region rgn = createRegion(name, factory.create());
            rgn.registerInterestRegex(".*", false, false);
          }
        };

    vm1.invoke(createEmpty);
    vm1.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "old" + i);
          }
        }
      });

    vm2.invoke(createNormal);
    vm1.invoke(new CacheSerializableRunnable("Turn on history") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          ctl.enableEventHistory();
        }
      });
    vm2.invoke(new CacheSerializableRunnable("Update region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), "new" + i, "callbackArg" + i);
          }
        }
      });
    pause(5 * 1000);

    vm1.invoke(new CacheSerializableRunnable("Verify invalidates") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          List l = ctl.getEventHistory();
          assertEquals(0, l.size());
        }
      });


    vm2.invoke(new CacheSerializableRunnable("Validate original and destroy") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            assertEquals("new" + i, region.getEntry(key).getValue());
            region.destroy(key, "destroyCB"+i);
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify destroys") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          List l = ctl.getEventHistory();
          assertEquals(0, l.size());
        }
      });
    vm2.invoke(new CacheSerializableRunnable("recreate") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            region.create(key, "create" + i, "createCB"+i);
          }
        }
      });
    pause(5 * 1000);
    
    vm1.invoke(new CacheSerializableRunnable("Verify creates") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
          List l = ctl.getEventHistory();
          assertEquals(0, l.size());
          // now see if we can get it from the server
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            assertEquals("create"+i, region.get(key, "loadCB"+i));
        }
          l = ctl.getEventHistory();
          assertEquals(10, l.size());
          for (int i = 0; i < 10; i++) {
            Object key = new Integer(i);
            EntryEvent ee = (EntryEvent)l.get(i);
            assertEquals(key, ee.getKey());
            assertEquals(null, ee.getOldValue());
            assertEquals("create"+i, ee.getNewValue());
            assertEquals(Operation.LOCAL_LOAD_CREATE, ee.getOperation());
            assertEquals("loadCB"+i, ee.getCallbackArgument());
            assertEquals(false, ee.isOriginRemote());
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  public AttributesFactory getBridgeServerRegionAttributes(CacheLoader cl, CacheWriter cw)
  {
    AttributesFactory ret = new AttributesFactory();
    if (cl != null) {
      ret.setCacheLoader(cl);
    }
    if (cw != null) {
      ret.setCacheWriter(cw);
    }
    ret.setScope(Scope.DISTRIBUTED_ACK);
    ret.setConcurrencyChecksEnabled(false);
    return ret;
  }

  public AttributesFactory getBridgeServerMirroredRegionAttributes(CacheLoader cl, CacheWriter cw)
  {
    AttributesFactory ret = new AttributesFactory();
    if (cl != null) {
      ret.setCacheLoader(cl);
    }
    if (cw != null) {
      ret.setCacheWriter(cw);
    }
    ret.setScope(Scope.DISTRIBUTED_NO_ACK);
    ret.setDataPolicy(DataPolicy.REPLICATE);
    ret.setConcurrencyChecksEnabled(false);

    return ret;
  }

  public AttributesFactory getBridgeServerMirroredAckRegionAttributes(CacheLoader cl, CacheWriter cw)
  {
    AttributesFactory ret = new AttributesFactory();
    if (cl != null) {
      ret.setCacheLoader(cl);
    }
    if (cw != null) {
      ret.setCacheWriter(cw);
    }
    ret.setScope(Scope.DISTRIBUTED_ACK);
    ret.setConcurrencyChecksEnabled(false);
    ret.setMirrorType(MirrorType.KEYS_VALUES);

    return ret;
  }

   /**
   * Tests that updates are not sent to VMs that did not ask for
   * them.
   */
  public void testOnlyRequestedUpdates() throws Exception {
    final String name1 = this.getName() + "-1";
    final String name2 = this.getName() + "-2";
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    // Cache server serves up both regions
    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null, null);
          createRegion(name1, factory.create());
          createRegion(name2, factory.create());
          //pause(1000);
        try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
        }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    // vm1 sends updates to the server
    vm1.invoke(new CacheSerializableRunnable("Create regions") {
        public void run2() throws CacheException {
          getLonerSystem();
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);

          BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);


          Region rgn = createRegion(name1, factory.create());
          rgn.registerInterestRegex(".*", false, false);
          rgn = createRegion(name2, factory.create());
          rgn.registerInterestRegex(".*", false, false);
          
      }
    });

    // vm2 only wants updates to updates to region1
    vm2.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          getLonerSystem();
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);

          BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);

          Region rgn = createRegion(name1, factory.create());
          rgn.registerInterestRegex(".*", false, false);
          createRegion(name2, factory.create());
          // no interest registration for region 2
        }
      });

    SerializableRunnable populate =
      new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region1 = getRootRegion().getSubregion(name1);
          for (int i = 0; i < 10; i++) {
            region1.put(new Integer(i), "Region1Old" + i);
          }
          Region region2 = getRootRegion().getSubregion(name2);
          for (int i = 0; i < 10; i++) {
            region2.put(new Integer(i), "Region2Old" + i);
    }
        }
      };
    vm1.invoke(populate);
    vm2.invoke(populate);

    vm1.invoke(new CacheSerializableRunnable("Update") {
        public void run2() throws CacheException {
          Region region1 = getRootRegion().getSubregion(name1);
          for (int i = 0; i < 10; i++) {
            region1.put(new Integer(i), "Region1New" + i);
          }
          Region region2 = getRootRegion().getSubregion(name2);
          for (int i = 0; i < 10; i++) {
            region2.put(new Integer(i), "Region2New" + i);
          }
        }
      });

    // Wait for updates to be propagated
    pause(5 * 1000);

    vm2.invoke(new CacheSerializableRunnable("Validate") {
        public void run2() throws CacheException {
          Region region1 = getRootRegion().getSubregion(name1);
          for (int i = 0; i < 10; i++) {
            assertEquals("Region1New" + i, region1.get(new Integer(i)));
          }
          Region region2 = getRootRegion().getSubregion(name2);
          for (int i = 0; i < 10; i++) {
            assertEquals("Region2Old" + i, region2.get(new Integer(i)));
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Close BridgeWriter") {
      public void run2() throws CacheException {
        // Terminate region1's BridgeWriter
        Region region1 = getRootRegion().getSubregion(name1);
        region1.localDestroyRegion();
        // Terminate region2's BridgeWriter
        Region region2 = getRootRegion().getSubregion(name2);
        region2.localDestroyRegion();
    }
    });

    vm2.invoke(new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
        // Terminate region1's BridgeWriter
        Region region1 = getRootRegion().getSubregion(name1);
        region1.localDestroyRegion();
        }
      });

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

 
  /**
   * Tests interest key registration.
   */
  public void testInterestKeyRegistration() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    // Create cache server
    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    // Create cache server clients
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
           createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(create);

    // Get values for key 1 and key 2 so that there are entries in the clients.
    // Register interest in one of the keys.
    vm1.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          assertEquals(region.get("key-1"), "key-1");
          assertEquals(region.get("key-2"), "key-2");
          try {
            region.registerInterest("key-1");
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          assertEquals(region.get("key-1"), "key-1");
          assertEquals(region.get("key-2"), "key-2");
          try {
            region.registerInterest("key-2");
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    // Put new values and validate updates (VM1)
    vm1.invoke(new CacheSerializableRunnable("Put New Values") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "vm1-key-1");
          region.put("key-2", "vm1-key-2");
          // Verify that no invalidates occurred to this region
          assertEquals(region.getEntry("key-1").getValue(), "vm1-key-1");
          assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2");
        }
      });

    pause(500);
    vm2.invoke(new CacheSerializableRunnable("Validate Entries") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // Verify that 'key-2' was updated, but 'key-1' was not
          // and contains the original value
          assertEquals(region.getEntry("key-1").getValue(), "key-1");
          assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2");
          //assertNull(region.getEntry("key-2").getValue());
        }
      });

    // Put new values and validate updates (VM2)
    vm2.invoke(new CacheSerializableRunnable("Put New Values") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "vm2-key-1");
          region.put("key-2", "vm2-key-2");
          // Verify that no updates occurred to this region
          assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
          assertEquals(region.getEntry("key-2").getValue(), "vm2-key-2");
        }
      });

    pause(500);
    vm1.invoke(new CacheSerializableRunnable("Validate Entries") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // Verify that 'key-1' was updated, but 'key-2' was not
          // and contains the original value
          assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2");
          assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
          //assertNull(region.getEntry("key-1").getValue());
        }
      });

    // Unregister interest
    vm1.invoke(new CacheSerializableRunnable("Unregister Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            region.unregisterInterest("key-1");
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Unregister Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            region.unregisterInterest("key-2");
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    // Put new values and validate updates (VM1)
    vm1.invoke(new CacheSerializableRunnable("Put New Values") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "vm1-key-1-again");
          region.put("key-2", "vm1-key-2-again");
          // Verify that no updates occurred to this region
          assertEquals(region.getEntry("key-1").getValue(), "vm1-key-1-again");
          assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2-again");
        }
      });

    pause(500);
    vm2.invoke(new CacheSerializableRunnable("Validate Entries") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // Verify that neither 'key-1' 'key-2' was updated
          // and contain the original value
          assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
          assertEquals(region.getEntry("key-2").getValue(), "vm2-key-2");
        }
      });

    // Put new values and validate updates (VM2)
    vm2.invoke(new CacheSerializableRunnable("Put New Values") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "vm2-key-1-again");
          region.put("key-2", "vm2-key-2-again");
          // Verify that no updates occurred to this region
          assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1-again");
          assertEquals(region.getEntry("key-2").getValue(), "vm2-key-2-again");
        }
      });

    pause(500);
    vm1.invoke(new CacheSerializableRunnable("Validate Entries") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // Verify that neither 'key-1' 'key-2' was updated
          // and contain the original value
          assertEquals(region.getEntry("key-1").getValue(), "vm1-key-1-again");
          assertEquals(region.getEntry("key-2").getValue(), "vm1-key-2-again");
        }
      });

    // Unregister interest again (to verify that a client can unregister interest
    // in a key that its not interested in with no problem.
    vm1.invoke(new CacheSerializableRunnable("Unregister Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            region.unregisterInterest("key-1");
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Unregister Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            region.unregisterInterest("key-2");
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    // Close cache server clients
    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  /**
   * Tests interest list registration.
   */
  public void testInterestListRegistration() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    // Create cache server
    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    // Create cache server clients
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
           createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(create);

    // Get values for key 1 and key 6 so that there are entries in the clients.
    // Register interest in a list of keys.
    vm1.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          assertEquals(region.get("key-1"), "key-1");
          assertEquals(region.get("key-6"), "key-6");
          try {
            List list = new ArrayList();
            list.add("key-1");
            list.add("key-2");
            list.add("key-3");
            list.add("key-4");
            list.add("key-5");
            region.registerInterest(list);
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          assertEquals(region.get("key-1"), "key-1");
          assertEquals(region.get("key-6"), "key-6");
        }
      });

    // Put new values and validate updates (VM2)
    vm2.invoke(new CacheSerializableRunnable("Put New Values") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "vm2-key-1");
          region.put("key-6", "vm2-key-6");
          // Verify that no updates occurred to this region
          assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
          assertEquals(region.getEntry("key-6").getValue(), "vm2-key-6");
        }
      });
    pause(5 * 1000);
    
    vm1.invoke(new CacheSerializableRunnable("Validate Entries") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // Verify that 'key-1' was updated
          assertEquals(region.getEntry("key-1").getValue(), "vm2-key-1");
          // Verify that 'key-6' was not invalidated
          assertEquals(region.getEntry("key-6").getValue(), "key-6");
        }
      });

    // Close cache server clients
    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }


  

  protected class ConnectionPoolDUnitTestSerializable2 implements java.io.Serializable
  {
    protected ConnectionPoolDUnitTestSerializable2(String key)
    {
      _key = key;
    }

    public String getKey()
    {
      return _key;
    }

    protected String _key;
  }

  /**
   * Accessed by reflection DO NOT REMOVE
   * @return
   */
  protected static int getCacheServerPort() {
    return bridgeServerPort;
  }

  protected static long getNumberOfAfterCreates() {
    return numberOfAfterCreates;
  }

  protected static long getNumberOfAfterUpdates() {
    return numberOfAfterUpdates;
  }

  protected static long getNumberOfAfterInvalidates() {
    return numberOfAfterInvalidates;
  }

  private class GetKey extends TestRunnable {

    private final String key;
    private Object result;
    private final String name;
    ConnectionPoolDUnitTest test;
    int repCount;
    private AB timeToStop; // if non-null then ignroe repCount

    protected GetKey(String objectName, ConnectionPoolDUnitTest t, String name, AB timeToStop) {
      this.key = objectName;
      this.test = t;
      this.name = name;
      this.timeToStop = timeToStop;
    }

    protected GetKey(String objectName, ConnectionPoolDUnitTest t, String name, int repCount) {
      this.key = objectName;
      this.test = t;
      this.name = name;
      this.repCount=repCount;
    }
    public void runTest() throws Throwable {
      if (this.timeToStop != null) {
        getUntilStopped();
      } else {
        getForRepCount();
      }
      //test.close();
    }

    private void getForRepCount() throws Throwable {
//      boolean killed = false;
      final Region r = test.getRootRegion().getSubregion(this.name);
      final PoolImpl pi = (PoolImpl)PoolManager.find(r.getAttributes().getPoolName());
      try {
      for (int i=0;i<repCount;i++) {
        try {
          String key = this.key + i;
          if (r.getEntry(key) != null) {
            r.localInvalidate(key);
          }
          result = r.get(key);
          assertTrue("GetKey after get " + key + " result=" + result, pi.getConnectedServerCount() >= 1);
          Thread.sleep(10);
        }
        catch(InterruptedException ie) {
          fail("interrupted");
        }
        catch(ServerConnectivityException sce) {
            DistributedTestCase.fail("While getting value for ACK region", sce);
        }
        catch(TimeoutException te) {
          DistributedTestCase.fail("While getting value for ACK region", te);
        }
      }
      assertTrue(pi.getConnectedServerCount() >= 1);
      } finally {
        pi.releaseThreadLocalConnection();
      }
    }

    private void getUntilStopped() throws Throwable {
//      boolean killed = false;
      final Region r = test.getRootRegion().getSubregion(this.name);
      final PoolImpl pi = (PoolImpl)PoolManager.find(r.getAttributes().getPoolName());
      try {
      int i=0;
      while (!timeToStop.get()) {
        i++;
        try {
          String key = this.key + i;
          if (r.getEntry(key) != null) {
            r.localInvalidate(key);
          }
          result = r.get(key);
          assertTrue("GetKey after get " + key + " result=" + result, pi.getConnectedServerCount() >= 1);
          Thread.sleep(10);
        }
        catch(InterruptedException ie) {
          fail("interrupted");
        }
        catch(ServerConnectivityException sce) {
            DistributedTestCase.fail("While getting value for ACK region", sce);
        }
        catch(TimeoutException te) {
          DistributedTestCase.fail("While getting value for ACK region", te);
        }
      }
      assertTrue(pi.getConnectedServerCount() >= 1);
      } finally {
        pi.releaseThreadLocalConnection();
      }
    }
  }

  /**
   * Creates a "loner" distributed system that has dynamic region creation
   * enabled.
   * @since 4.3
   */
  protected Cache createDynamicRegionCache(String testName, BridgeWriter w) {
    // note that clients use non-persistent dr factories.
    
    DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(null, w, false));
    getLogWriter().info("CREATED IT");
    Cache z = getCache();
    return z;
  }

  /**
   * Creates a "loner" distributed system that has dynamic region creation
   * enabled.
   * @since 4.3
   */
  protected Cache createDynamicRegionCache(String testName, String connectionPoolName) {
    // note that clients use non-persistent dr factories.
    
    DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(null,connectionPoolName,false,true));
    getLogWriter().info("CREATED IT");
    Cache z = getCache();
    return z;
  }
  /**
   * A handy method to poll for arrival of non-null/non-invalid entries
   * @param r the Region to poll
   * @param key  the key of the Entry to poll for
   */
  public static void waitForEntry(final Region r, final Object key) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return r.containsValueForKey(key);
      }
      public String description() {
        return "Waiting for entry " + key + " on region " + r;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
  }

  public static Region waitForSubRegion(final Region r, final String subRegName) {
//    final long start = System.currentTimeMillis();
    final long MAXWAIT = 10000;
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return r.getSubregion(subRegName) != null;
      }
      public String description() {
        return "Waiting for subregion " + subRegName;
      }
    };
    DistributedTestCase.waitForCriterion(ev, MAXWAIT, 200, true);
    Region result = r.getSubregion(subRegName);
    return result;
  }

  /**
   * Create a bridgeserver that has a value for every key queried and a unique
   * key/value in the specified Region that uniquely identifies each instance.
   *
   * @param vm
   *          the VM on which to create the BridgeServer
   * @param rName
   *          the name of the Region to create on the BridgeServerf
   * @param port
   *          the TCP port on which the BridgeServer should listen
   */
  public static class BridgeServerCacheLoader extends TestCacheLoader implements Declarable {

    public BridgeServerCacheLoader() {}

    public Object load2(LoaderHelper helper) {
      if (helper.getArgument() instanceof Integer) {
        try {
          Thread.sleep(((Integer) helper.getArgument()).intValue());
        }
        catch (InterruptedException ugh) { fail("interrupted"); }
      }
      return helper.getKey();
    }

    public void init(Properties props)  {}
  }
  public void createBridgeServer(VM vm, final String rName, final int port, final boolean notifyBySubscription) {
      vm.invoke(new CacheSerializableRunnable("Create Region on Server") {
      public void run2() {
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          factory.setCacheLoader(new BridgeServerCacheLoader());
          beginCacheXml();
          createRegion(rName, factory.create());
          startBridgeServer(port);
          finishCacheXml(rName + "-" + port);

          Region region = getRootRegion().getSubregion(rName);
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(rName));
          region.put("BridgeServer", new Integer(port)); // A unique key/value to identify the BridgeServer
        }
        catch(Exception e) {
          getSystem().getLogWriter().severe(e);
          fail("Failed to start CacheServer " + e);
        }
      }
    });
  }

//test for bug 35884
  public void testClientGetOfInvalidServerEntry() throws CacheException {
    final String regionName1 = this.getName() + "-1";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(2);

    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(regionName1, factory.create());

        pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    };

    // Create server1.
    server1.invoke(createServer);

    final int port = server1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server1.getHost());

    // Init values at server.
    server1.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName1);
        // create it invalid
        region1.create("key-string-1", null);
      }
    });

    // now try it with a local scope

    SerializableRunnable createBridgeLoader2 =
      new CacheSerializableRunnable("Create region 2") {
      public void run2() throws CacheException {
//        Region region1 = getRootRegion().getSubregion(regionName1);
//        region1.localDestroyRegion();
        getLonerSystem();
        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        regionFactory.setConcurrencyChecksEnabled(false);
        getLogWriter().info("ZZZZZ host0:"+host0+" port:"+port);
        BridgeTestCase.configureConnectionPool(regionFactory,host0,port,-1,false,-1,-1, null);
        getLogWriter().info("ZZZZZDone host0:"+host0+" port:"+port);
        createRegion(regionName1, regionFactory.create());
      }
    };
    client.invoke(createBridgeLoader2);

    // get the invalid entry on the client.
    client.invoke(new CacheSerializableRunnable("get values on client") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName1);
        assertEquals(null, region1.getEntry("key-string-1"));
        assertEquals(null, region1.get("key-string-1"));
      }
    });

    server1.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });

  }

  
  public void testClientRegisterUnregisterRequests() throws CacheException {
    final String regionName1 = this.getName() + "-1";

    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(2);

    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        factory.setConcurrencyChecksEnabled(false);
        createRegion(regionName1, factory.create());

        //pause(1000);
        try {
          startBridgeServer(0);

        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }

      }
    };

    // Create server1.
    server1.invoke(createServer);

    final int port = server1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server1.getHost());

    SerializableRunnable createBridgeWriter =
      new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        getLonerSystem();
        getCache();
        Region region1 = null;

        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        regionFactory.setConcurrencyChecksEnabled(false);

        BridgeTestCase.configureConnectionPool(regionFactory,host0,port,-1,true,-1,-1, null);

        region1 = createRegion(regionName1, regionFactory.create());
        region1.getAttributesMutator().setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
      }
    };

    // Create client.
    client.invoke(createBridgeWriter);

    // Init values at server.
    server1.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName1);
        for (int i = 0; i < 20; i++) {
          region1.put("key-string-"+i, "value-"+i);
        }
      }
    });


    // Put some values on the client.
    client.invoke(new CacheSerializableRunnable("Put values client") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName1);

        for (int i = 0; i < 10; i++) {
          region1.put("key-string-"+i, "client-value-"+i);
        }
      }
    });

    SerializableRunnable closeBridgeWriter =
      new CacheSerializableRunnable("Close BridgeWriter") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName1);
        String pName = region1.getAttributes().getPoolName();
        region1.localDestroyRegion();
        PoolImpl p = (PoolImpl)PoolManager.find(pName);
        p.destroy();
      }
    };

    client.invoke(closeBridgeWriter);

    SerializableRunnable validateClientRegisterUnRegister =
      new CacheSerializableRunnable("validate Client Register UnRegister") {
      public void run2() throws CacheException {
        for (Iterator bi = getCache().getCacheServers().iterator(); bi.hasNext(); ) {
          BridgeServerImpl bsi = (BridgeServerImpl) bi.next();
          final CacheClientNotifierStats ccnStats = bsi.getAcceptor().getCacheClientNotifier().getStats();
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return ccnStats.getClientRegisterRequests() == ccnStats.getClientUnRegisterRequests();
            }
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev, 62 * 1000, 200, true);
          assertEquals("HealthMonitor Client Register/UnRegister mismatch.", ccnStats.getClientRegisterRequests(), ccnStats.getClientUnRegisterRequests());
        }
      }
    };

    server1.invoke(validateClientRegisterUnRegister);

    server1.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });

  }

  /**
   * Tests the containsKeyOnServer operation of the {@link BridgeWriter}
   *
   * @since 5.0.2
   */
  public void testContainsKeyOnServer() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,false,-1,-1, null);
            createRegion(name, factory.create());
          }
        };
    vm1.invoke(create);
    vm2.invoke(create);

    final Integer key1 = new Integer(0);
    final String key2 = "0";
    vm2.invoke(new CacheSerializableRunnable("Contains key on server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        boolean containsKey = false;
        containsKey = region.containsKeyOnServer(key1);
        assertFalse(containsKey);
        containsKey = region.containsKeyOnServer(key2);
        assertFalse(containsKey);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Put values") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.put(new Integer(0), new Integer(0));
          region.put("0", "0");
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Contains key on server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        boolean containsKey = false;
        containsKey = region.containsKeyOnServer(key1);
        assertTrue(containsKey);
        containsKey = region.containsKeyOnServer(key2);
        assertTrue(containsKey);
      }
    });

    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };
    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  
  /**
   * Tests that invoking {@link Region#create} with a
   * <code>null</code> value does the right thing with the {@link
   * BridgeWriter}.
   *
   * @since 3.5
   */
  public void testCreateNullValue() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null,null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);

            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };
    vm1.invoke(create);

    vm2.invoke(create);
    vm2.invoke(new CacheSerializableRunnable("Create nulls") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.create(new Integer(i), null, createCallbackArg);
          }
        }
      });

    pause(1000);                // Wait for updates to be propagated

    vm2.invoke(new CacheSerializableRunnable("Verify invalidates") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
            for (int i = 0; i < 10; i++) {
              Region.Entry entry = region.getEntry(new Integer(i));
              assertNotNull(entry);
              assertNull(entry.getValue());
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Attempt to create values") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
      region.create(new Integer(i), "new" + i);
          }
        }
      });

    pause(1000);                // Wait for updates to be propagated

    vm2.invoke(new CacheSerializableRunnable("Verify invalidates") {
          public void run2() throws CacheException {
            Region region = getRootRegion().getSubregion(name);
            for (int i = 0; i < 10; i++) {
              Region.Entry entry = region.getEntry(new Integer(i));
              assertNotNull(entry);
              assertNull(entry.getValue());
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  
  
  /**
   * Tests that a {@link Region#localDestroy} is not propagated to the
   * server and that a {@link Region#destroy} is.  Also makes sure
   * that callback arguments are passed correctly.
   */
  public void testDestroy() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final Object callbackArg = "DESTROY CALLBACK";

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {

          CacheWriter cw = new TestCacheWriter() {
              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException {

              }

              public void beforeDestroy2(EntryEvent event)
                throws CacheWriterException {
                Object beca = event.getCallbackArgument();
                assertEquals(callbackArg, beca);
              }
            };
          AttributesFactory factory = getBridgeServerRegionAttributes(null, cw);
          createRegion(name, factory.create());
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);

            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);

            Region rgn = createRegion(name, factory.create());
            rgn.registerInterestRegex(".*", false, false);
            }
        };
    vm1.invoke(create);
    vm1.invoke(new CacheSerializableRunnable("Populate region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), String.valueOf(i));
          }
        }
      });

    vm2.invoke(create);
    vm2.invoke(new CacheSerializableRunnable("Load region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            assertEquals(String.valueOf(i), region.get(new Integer(i)));
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Local destroy") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.localDestroy(new Integer(i));
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("No destroy propagate") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            assertEquals(String.valueOf(i), region.get(new Integer(i)));
          }
          }
      });

    vm1.invoke(new CacheSerializableRunnable("Fetch from server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            assertEquals(String.valueOf(i), region.get(new Integer(i)));
          }
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Check no server cache writer") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          TestCacheWriter writer = getTestWriter(region);
          writer.wasInvoked();
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Distributed destroy") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.destroy(new Integer(i), callbackArg);
          }
        }
      });
    pause(1000);                // Wait for destroys to propagate

    vm1.invoke(new CacheSerializableRunnable("Attempt get from server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            assertNull(region.getEntry(new Integer(i)));
            }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Validate destroy propagate") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            assertNull(region.getEntry(new Integer(i)));
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    };

    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  /**
   * Tests that a {@link Region#localDestroyRegion} is not propagated
   * to the server and that a {@link Region#destroyRegion} is.  Also
   * makes sure that callback arguments are passed correctly.
   */
  public void todo_testDestroyRegion() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final Object callbackArg = "DESTROY CALLBACK";

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {

          CacheWriter cw = new TestCacheWriter() {
              public void beforeCreate2(EntryEvent event)
                throws CacheWriterException {

              }

              public void beforeRegionDestroy2(RegionEvent event)
                throws CacheWriterException {

                assertEquals(callbackArg,event.getCallbackArgument());
              }
            };
            AttributesFactory factory = getBridgeServerRegionAttributes(null, cw);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Local destroy region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
          assertNull(getRootRegion().getSubregion(name));
          // close the bridge writer to prevent callbacks on the connections
          // Not necessary since locally destroying the region takes care of this.
          //getPoolClient(region).close();
            }
      });

    vm2.invoke(new CacheSerializableRunnable("No destroy propagate") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          assertNotNull(region);
          }
      });

    vm0.invoke(new CacheSerializableRunnable("Check no server cache writer") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          TestCacheWriter writer = getTestWriter(region);
          writer.wasInvoked();
        }
      });

    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Distributed destroy region") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          assertNotNull(region);
          region.destroyRegion(callbackArg);
          assertNull(getRootRegion().getSubregion(name));
          // close the bridge writer to prevent callbacks on the connections
          // Not necessary since locally destroying the region takes care of this.
          //getPoolClient(region).close();
          }
      });
    pause(1000);                // Wait for destroys to propagate

    vm2.invoke(new CacheSerializableRunnable("Verify destroy propagate") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          assertNull(region);
          // todo close the bridge writer
          // Not necessary since locally destroying the region takes care of this.
        }
      });

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
      public void run() {
          stopBridgeServer(getCache());
      }
    });

  }

  
  /**
   * Tests interest list registration with callback arg with DataPolicy.EMPTY
   * and InterestPolicy.ALL
   */
  public void testDPEmptyInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    // Create cache server
    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);
          createRegion(name, factory.create());
          pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    // Create cache server clients
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            factory.addCacheListener(new ControlListener());
            factory.setDataPolicy(DataPolicy.EMPTY);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
            createRegion(name, factory.create());
          }
        };
    SerializableRunnable createPublisher =
      new CacheSerializableRunnable("Create publisher region") {
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            factory.addCacheListener(new ControlListener());
            factory.setDataPolicy(DataPolicy.EMPTY); // make sure empty works with client publishers
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(createPublisher);

    // VM1 Register interest
    vm1.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            // This call will cause no value to be put into the region
            region.registerInterest("key-1", InterestResultPolicy.NONE);
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke(new CacheSerializableRunnable("Put Value") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.create("key-1", "key-1-create", "key-1-create");
        }
      });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke(new CacheSerializableRunnable("Put Value") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "key-1-update", "key-1-update");
        }
      });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke(new CacheSerializableRunnable("Destroy Entry") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.destroy("key-1", "key-1-destroy");
        }
      });

    final SerializableRunnable assertEvents =
      new CacheSerializableRunnable("Verify events") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
          int eventCount = 3;
          listener.waitWhileNotEnoughEvents(60000, eventCount);
          assertEquals(eventCount, listener.events.size());

          {
            EventWrapper ew = (EventWrapper)listener.events.get(0);
            assertEquals(TYPE_CREATE, ew.type);
            Object key = "key-1";
            assertEquals(key, ew.event.getKey());
            assertEquals(null, ew.event.getOldValue());
            assertEquals(false, ew.event.isOldValueAvailable()); // failure
            assertEquals("key-1-create", ew.event.getNewValue());
            assertEquals(Operation.CREATE, ew.event.getOperation());
            assertEquals("key-1-create", ew.event.getCallbackArgument());
            assertEquals(true, ew.event.isOriginRemote());

            ew = (EventWrapper)listener.events.get(1);
            assertEquals(TYPE_UPDATE, ew.type);
            assertEquals(key, ew.event.getKey());
            assertEquals(null, ew.event.getOldValue());
            assertEquals(false, ew.event.isOldValueAvailable());
            assertEquals("key-1-update", ew.event.getNewValue());
            assertEquals(Operation.UPDATE, ew.event.getOperation());
            assertEquals("key-1-update", ew.event.getCallbackArgument());
            assertEquals(true, ew.event.isOriginRemote());

            ew = (EventWrapper)listener.events.get(2);
            assertEquals(TYPE_DESTROY, ew.type);
            assertEquals("key-1-destroy", ew.arg);
            assertEquals(key, ew.event.getKey());
            assertEquals(null, ew.event.getOldValue());
            assertEquals(false, ew.event.isOldValueAvailable());
            assertEquals(null, ew.event.getNewValue());
            assertEquals(Operation.DESTROY, ew.event.getOperation());
            assertEquals("key-1-destroy", ew.event.getCallbackArgument());
            assertEquals(true, ew.event.isOriginRemote());
          }
        }
      };
    vm1.invoke(assertEvents);

    // Close cache server clients
    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }
  /**
   * Tests interest list registration with callback arg with DataPolicy.EMPTY
   * and InterestPolicy.CACHE_CONTENT
   */
  public void testDPEmptyCCInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    // Create cache server
    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    // Create cache server clients
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            factory.setCacheListener(new ControlListener());
            factory.setDataPolicy(DataPolicy.EMPTY);
            factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT));
            createRegion(name, factory.create());
          }
        };
    SerializableRunnable createPublisher =
      new CacheSerializableRunnable("Create publisher region") {
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            factory.setCacheListener(new ControlListener());
            factory.setDataPolicy(DataPolicy.EMPTY); // make sure empty works with client publishers
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(createPublisher);

    // VM1 Register interest
    vm1.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            // This call will cause no value to be put into the region
            region.registerInterest("key-1", InterestResultPolicy.NONE);
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke(new CacheSerializableRunnable("Put Value") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.create("key-1", "key-1-create", "key-1-create");
        }
      });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke(new CacheSerializableRunnable("Put Value") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "key-1-update", "key-1-update");
        }
      });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke(new CacheSerializableRunnable("Destroy Entry") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.destroy("key-1", "key-1-destroy");
        }
      });

    final SerializableRunnable assertEvents =
      new CacheSerializableRunnable("Verify events") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
          pause(1000); // we should not get any events but give some time for the server to send them
          assertEquals(0, listener.events.size());
        }
      };
      vm1.invoke(assertEvents);

    // Close cache server clients
    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }

  /**
   * Test dynamic region creation instantiated from a bridge client
   * causing regions to be created on two different bridge servers.
   *
   * Also tests the reverse situation, a dynamic region is created on the
   * bridge server expecting the same region to be created on the client.
   *
   * Note: This test re-creates Distributed Systems for its own purposes
   * and uses a Loner distributed systems to isolate the Bridge Client.
   *
   * @throws Exception
   */
  public void testDynamicRegionCreation() throws Exception {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    final VM client1 = host.getVM(0);
    // VM client2 = host.getVM(1);
    final VM srv1 = host.getVM(2);
    final VM srv2 = host.getVM(3);

    final String k1 = name + "-key1";
    final String v1 = name + "-val1";
    final String k2 = name + "-key2";
    final String v2 = name + "-val2";
    final String k3 = name + "-key3";
    final String v3 = name + "-val3";

    client1.invoke(DistributedTestCase.class, "disconnectFromDS");
    srv1.invoke(DistributedTestCase.class, "disconnectFromDS");
    srv2.invoke(DistributedTestCase.class, "disconnectFromDS");
    try {
      // setup servers
      CacheSerializableRunnable ccs = new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          createDynamicRegionCache(name, (BridgeWriter)null); // Creates a new DS and Cache
          assertTrue(DynamicRegionFactory.get().isOpen());
          try {
            startBridgeServer(0);
          } catch (IOException ugh) {
            fail("Bridge Server startup failed");
          }
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setConcurrencyChecksEnabled(false);
          Region region = createRootRegion(name, factory.create());
          region.put(k1, v1);
          Assert.assertTrue(region.get(k1).equals(v1));
        }
      };
      srv1.invoke(ccs);
      srv2.invoke(ccs);

      final String srv1Host = getServerHostName(srv1.getHost());
      final int srv1Port =
        srv1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");

      final int srv2Port =
        srv2.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
//      final String srv2Host = getServerHostName(srv2.getHost());

      // setup clients, do basic tests to make sure the BridgeWriter with notifier and the BridgeLoader work
      // as advertised
      client1.invoke(new CacheSerializableRunnable("Create Cache Client") {
        public void run2() throws CacheException {
          createLonerDS();
          AttributesFactory factory = new AttributesFactory();
          factory.setConcurrencyChecksEnabled(false);
          Pool cp = BridgeTestCase.configureConnectionPool(factory,srv1Host,srv1Port,srv2Port,true,-1,-1, null);
          {
            final PoolImpl pool = (PoolImpl)cp;
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                if (pool.getPrimary() == null) {
                  return false;
                }
                if (pool.getRedundants().size() < 1) {
                  return false;
                }
                return true;
              }
              public String description() {
                return null;
              }
            };
            DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
            assertNotNull(pool.getPrimary());
            assertTrue("backups="+pool.getRedundants() + " expected=" + 1,
                       pool.getRedundants().size() >= 1);
          }
          
          createDynamicRegionCache(name, "testPool");

          assertTrue(DynamicRegionFactory.get().isOpen());
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          factory.setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
          Region region = createRootRegion(name, factory.create());

          
          assertNull(region.getEntry(k1));
          region.registerInterestRegex(".*", InterestResultPolicy.KEYS_VALUES);  // this should match the key
          assertEquals(v1, region.getEntry(k1).getValue()); // Update via registered interest

          assertNull(region.getEntry(k2));
          region.put(k2, v2);  // use the BridgeWriter
          assertEquals(v2, region.getEntry(k2).getValue()); // Ensure that the notifier didn't un-do the put, bug 35355

          region.put(k3, v3); // setup a key for invalidation from a notifier
        }
      });

      srv1.invoke(new CacheSerializableRunnable("Validate Server1 update") {
        public void run2() throws CacheException {
          CacheClientNotifier ccn = CacheClientNotifier.getInstance();
          final CacheClientNotifierStats ccnStats = ccn.getStats();
          final int eventCount = ccnStats.getEvents();
          Region r = getRootRegion(name);
          assertNotNull(r);
          assertEquals(v2, r.getEntry(k2).getValue()); // Validate the BridgeWriter worked, getEntry works because of the mirror
          assertEquals(v3, r.getEntry(k3).getValue()); // Make sure we have the other entry to use for notification
          r.put(k3, v1); // Change k3, sending some data to the client notifier

          // Wait for the update to propagate to the clients
          final int maxTime = 20000;
//            long start = System.currentTimeMillis();
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return ccnStats.getEvents() > eventCount;
            }
            public String description() {
              return "waiting for ccnStat";
            }
          };
          DistributedTestCase.waitForCriterion(ev, maxTime, 200, true);
//            Set prox = ccn.getClientProxies();
//            assertEquals(1, prox.size());
//            for (Iterator cpi = prox.iterator(); cpi.hasNext(); ) {
//              CacheClientProxy ccp = (CacheClientProxy) cpi.next();
//              start = System.currentTimeMillis();
//              while (ccp.getMessagesProcessed() < 1) {
//                assertTrue("Waited more than " + maxTime + "ms for client notification", (System.currentTimeMillis() - start) < maxTime);
//                try {
//                  Thread.sleep(100);
//                } catch (InterruptedException ine) { fail("Interrupted while waiting for client notifier to complete"); }
//              }
//            }
        }
      });
      srv2.invoke(new CacheSerializableRunnable("Validate Server2 update") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          assertEquals(v2, r.getEntry(k2).getValue()); // Validate the BridgeWriter worked, getEntry works because of the mirror
          assertEquals(v1, r.getEntry(k3).getValue()); // From peer update
        }
      });
      client1.invoke(new CacheSerializableRunnable("Validate Client notification") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) r.getAttributes().getCacheListener();
          ctl.waitForUpdated(k3);
          assertEquals(v1, r.getEntry(k3).getValue()); // Ensure that the notifier updated the entry
        }
      });
      // Ok, now we are ready to do some dynamic region action!
      final String v1Dynamic = v1 + "dynamic";
      final String dynFromClientName = name + "-dynamic-client";
      final String dynFromServerName = name + "-dynamic-server";
      client1.invoke(new CacheSerializableRunnable("Client dynamic region creation") {
        public void run2() throws CacheException {
          assertTrue(DynamicRegionFactory.get().isOpen());
          Region r = getRootRegion(name);
          assertNotNull(r);
          Region dr = DynamicRegionFactory.get().createDynamicRegion(name, dynFromClientName);
          assertNull(dr.get(k1)); // This should be enough to validate the creation on the server
          dr.put(k1, v1Dynamic);
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // Assert the servers have the dynamic region and the new value
      CacheSerializableRunnable valDR = new CacheSerializableRunnable("Validate dynamic region creation on server") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          long end = System.currentTimeMillis() + 10000;
          Region dr = null;
          for (;;) {
            try {
              dr = r.getSubregion(dynFromClientName);
              assertNotNull(dr);
              assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynFromClientName));
              break;
            }
            catch (AssertionFailedError e) {
              if (System.currentTimeMillis() > end) {
                throw e;
              }
            }
          }

          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      };
      srv1.invoke(valDR);
      srv2.invoke(valDR);
      // now delete the dynamic region and see if it goes away on servers
      client1.invoke(new CacheSerializableRunnable("Client dynamic region destruction") {
        public void run2() throws CacheException {
          assertTrue(DynamicRegionFactory.get().isActive());
          Region r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromClientName;

          assertNotNull(getCache().getRegion(drName));
          DynamicRegionFactory.get().destroyDynamicRegion(drName);
          assertNull(getCache().getRegion(drName));
        }
      });
      // Assert the servers no longer have the dynamic region
      CacheSerializableRunnable valNoDR = new CacheSerializableRunnable("Validate dynamic region destruction on server") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromClientName;
          assertNull(getCache().getRegion(drName));
          try {
            DynamicRegionFactory.get().destroyDynamicRegion(drName);
            fail("expected RegionDestroyedException");
          } catch (RegionDestroyedException expected) {
          }
        }
      };
      srv1.invoke(valNoDR);
      srv2.invoke(valNoDR);
      // Now try the reverse, create a dynamic region on the server and see if the client
      // has it
      srv2.invoke(new CacheSerializableRunnable("Server dynamic region creation") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          Region dr = DynamicRegionFactory.get().createDynamicRegion(name, dynFromServerName);
          assertNull(dr.get(k1));
          dr.put(k1, v1Dynamic);
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // Assert the servers have the dynamic region and the new value
      srv1.invoke(new CacheSerializableRunnable("Validate dynamic region creation propagation to other server") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          Region dr = waitForSubRegion(r, dynFromServerName);
          assertNotNull(dr);
          assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynFromServerName));
          waitForEntry(dr, k1);
          assertNotNull(dr.getEntry(k1));
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // Assert the clients have the dynamic region and the new value
      client1.invoke(new CacheSerializableRunnable("Validate dynamic region creation on client") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          long end = System.currentTimeMillis() + 10000;
          Region dr = null;
          for (;;) {
            try {
              dr = r.getSubregion(dynFromServerName);
              assertNotNull(dr);
              assertNotNull(getCache().getRegion(name + Region.SEPARATOR + dynFromServerName));
              break;
            }
            catch (junit.framework.AssertionFailedError e) {
              if (System.currentTimeMillis() > end) {
                throw e;
              } else {
                pause(1000);
              }
            }
          }
          waitForEntry(dr, k1);
          assertNotNull(dr.getEntry(k1));
          assertEquals(v1Dynamic, dr.getEntry(k1).getValue());
        }
      });
      // now delete the dynamic region on a server and see if it goes away on client
      srv2.invoke(new CacheSerializableRunnable("Server dynamic region destruction") {
        public void run2() throws CacheException {
          assertTrue(DynamicRegionFactory.get().isActive());
          Region r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromServerName;

          assertNotNull(getCache().getRegion(drName));
          DynamicRegionFactory.get().destroyDynamicRegion(drName);
          assertNull(getCache().getRegion(drName));
        }
      });
      srv1.invoke(new CacheSerializableRunnable("Validate dynamic region destruction on other server") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromServerName;
          {
            int retry = 100;
            while (retry-- > 0 && getCache().getRegion(drName) != null) {
              try { Thread.sleep(100); } catch (InterruptedException ignore) {fail("interrupted");}
            }
          }
          assertNull(getCache().getRegion(drName));
        }
      });
      // Assert the clients no longer have the dynamic region
      client1.invoke(new CacheSerializableRunnable("Validate dynamic region destruction on client") {
        public void run2() throws CacheException {
          Region r = getRootRegion(name);
          assertNotNull(r);
          String drName = r.getFullPath() + Region.SEPARATOR + dynFromServerName;
          {
            int retry = 100;
            while (retry-- > 0 && getCache().getRegion(drName) != null) {
              try { Thread.sleep(100); } catch (InterruptedException ignore) {fail("interrupted");}
            }
          }
          assertNull(getCache().getRegion(drName));
          //sleep to make sure that the dynamic region entry from the internal region,dynamicRegionList in DynamicRegionFactory // ?
          try { Thread.sleep(10000); } catch (InterruptedException ignore) {fail("interrupted");}
          try {
            DynamicRegionFactory.get().destroyDynamicRegion(drName);
            fail("expected RegionDestroyedException");
          } catch (RegionDestroyedException expected) {
          }
        }
      });
    } finally {
      client1.invoke(DistributedTestCase.class, "disconnectFromDS"); // clean-up loner
      srv1.invoke(DistributedTestCase.class, "disconnectFromDS");
      srv2.invoke(DistributedTestCase.class, "disconnectFromDS");
    }
  }
  
  
  /**
   * Test for bug 36279
   */
  public void testEmptyByteArray() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final Object createCallbackArg = "CREATE CALLBACK ARG";

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null,null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          getLonerSystem();
          getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);

          BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
          createRegion(name, factory.create());
        }
      };

    vm1.invoke(create);
    vm1.invoke(new CacheSerializableRunnable("Create empty byte array") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 1; i++) {
            region.create(new Integer(i), new byte[0], createCallbackArg);
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify values on client") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 1; i++) {
            Region.Entry entry = region.getEntry(new Integer(i));
            assertNotNull(entry);
            byte[] value = (byte[])entry.getValue();
            assertNotNull(value);
            assertEquals(0, value.length);
          }
        }
      });
    vm0.invoke(new CacheSerializableRunnable("Verify values on server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 1; i++) {
            Region.Entry entry = region.getEntry(new Integer(i));
            assertNotNull(entry);
            byte[] value = (byte[])entry.getValue();
            assertNotNull(value);
            assertEquals(0, value.length);
          }
        }
      });

    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };

    vm1.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
      });
  }

  
  /**
   * Tests interest list registration with callback arg
   */
  public void testInterestListRegistrationWithCallbackArg() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);

    // Create cache server
    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });

    // Create cache server clients
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            // create bridge writer
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            factory.setCacheListener(new ControlListener());
            createRegion(name, factory.create());
          }
        };

    vm1.invoke(create);
    vm2.invoke(create);

    // VM1 Register interest
    vm1.invoke(new CacheSerializableRunnable("Create Entries and Register Interest") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          try {
            // This call will cause no value to be put into the region
            region.registerInterest("key-1", InterestResultPolicy.NONE);
          } catch (Exception ex) {
            fail("While registering interest: ", ex);
          }
        }
      });

    // VM2 Put entry (this will cause a create event in both VM1 and VM2)
    vm2.invoke(new CacheSerializableRunnable("Put Value") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.create("key-1", "key-1-create", "key-1-create");
        }
      });

    // VM2 Put entry (this will cause an update event in both VM1 and VM2)
    vm2.invoke(new CacheSerializableRunnable("Put Value") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.put("key-1", "key-1-update", "key-1-update");
        }
      });

    // VM2 Destroy entry (this will cause a destroy event)
    vm2.invoke(new CacheSerializableRunnable("Destroy Entry") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.destroy("key-1", "key-1-destroy");
        }
      });

    final SerializableRunnable assertEvents =
      new CacheSerializableRunnable("Verify events") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          ControlListener listener = (ControlListener) region.getAttributes().getCacheListeners()[0];
          int eventCount = 3;
          listener.waitWhileNotEnoughEvents(60000, eventCount);
          assertEquals(eventCount, listener.events.size());

          {
            EventWrapper ew = (EventWrapper)listener.events.get(0);
            assertEquals(ew.type, TYPE_CREATE);
            Object key = "key-1";
            assertEquals(key, ew.event.getKey());
            assertEquals(null, ew.event.getOldValue());
            assertEquals("key-1-create", ew.event.getNewValue());
            assertEquals(Operation.CREATE, ew.event.getOperation());
            assertEquals("key-1-create", ew.event.getCallbackArgument());
            assertEquals(true, ew.event.isOriginRemote());

            ew = (EventWrapper)listener.events.get(1);
            assertEquals(ew.type, TYPE_UPDATE);
            assertEquals(key, ew.event.getKey());
            assertEquals("key-1-create", ew.event.getOldValue());
            assertEquals("key-1-update", ew.event.getNewValue());
            assertEquals(Operation.UPDATE, ew.event.getOperation());
            assertEquals("key-1-update", ew.event.getCallbackArgument());
            assertEquals(true, ew.event.isOriginRemote());

            ew = (EventWrapper)listener.events.get(2);
            assertEquals(ew.type, TYPE_DESTROY);
              assertEquals("key-1-destroy", ew.arg);
            assertEquals(key, ew.event.getKey());
            assertEquals("key-1-update", ew.event.getOldValue());
            assertEquals(null, ew.event.getNewValue());
            assertEquals(Operation.DESTROY, ew.event.getOperation());
            assertEquals("key-1-destroy", ew.event.getCallbackArgument());
            assertEquals(true, ew.event.isOriginRemote());
          }
        }
      };
      vm1.invoke(assertEvents);

    // Close cache server clients
    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };

    vm1.invoke(close);
    vm2.invoke(close);

    // Stop cache server
    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
            }
    });
  }

  
  /**
   * Tests the keySetOnServer operation of the {@link BridgeWriter}
   *
   * @since 5.0.2
   */
  public void testKeySetOnServer() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());
    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);
            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };
    vm1.invoke(create);
    vm2.invoke(create);

    vm2.invoke(new CacheSerializableRunnable("Get keys on server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        Set keySet = region.keySetOnServer();
        assertNotNull(keySet);
        assertEquals(0, keySet.size());
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Put values") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), new Integer(i));
          }
        }
      });

    vm2.invoke(new CacheSerializableRunnable("Get keys on server") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          Set keySet = region.keySetOnServer();
          assertNotNull(keySet);
          assertEquals(10, keySet.size());
        }
      });

    SerializableRunnable close =
      new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
      };
    vm1.invoke(close);
    vm2.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
        public void run() {
          stopBridgeServer(getCache());
        }
    });
  }
  
  
  // this test doesn't do anything so I commented it out
//   /**
//    * Tests that new connections update client notification connections.
//    */
//   public void testNewConnections() throws Exception {
//     final String name = this.getName();
//     final Host host = Host.getHost(0);
//     VM vm0 = host.getVM(0);
//     VM vm1 = host.getVM(1);
//     VM vm2 = host.getVM(2);

//     // Cache server serves up the region
//     vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
//         public void run2() throws CacheException {
//           AttributesFactory factory = getBridgeServerRegionAttributes(null,null);
//           Region region = createRegion(name, factory.create());
//           pause(1000);
//           try {
//             startBridgeServer(0);

//           } catch (Exception ex) {
//             fail("While starting CacheServer", ex);
//           }

//         }
//       });
//     final int port =
//       vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
//     final String host0 = getServerHostName(vm0.getHost());

//     SerializableRunnable create =
//       new CacheSerializableRunnable("Create region") {
//           public void run2() throws CacheException {
//             getCache();
//             AttributesFactory factory = new AttributesFactory();
//             factory.setScope(Scope.LOCAL);

//             BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);

//             createRegion(name, factory.create());
//           }
//         };

//     vm1.invoke(create);
//     vm2.invoke(create);

//     vm1.invoke(new CacheSerializableRunnable("Create new connection") {
//         public void run2() throws CacheException {
//           Region region = getRootRegion().getSubregion(name);
//           BridgeClient writer = getPoolClient(region);
//           Endpoint[] endpoints = (Endpoint[])writer.getEndpoints();
//           for (int i=0; i<endpoints.length; i++) endpoints[i].addNewConnection();
//         }
//       });

//   SerializableRunnable close =
//     new CacheSerializableRunnable("Close BridgeWriter") {
//         public void run2() throws CacheException {
//           Region region = getRootRegion().getSubregion(name);
//           region.localDestroyRegion();
//         }
//     };

//     vm1.invoke(close);
//     vm2.invoke(close);

//     vm0.invoke(new SerializableRunnable("Stop CacheServer") {
//         public void run() {
//           stopBridgeServer(getCache());
//         }
//     });
//   }

  /**
   * Tests that creating, putting and getting a non-serializable key or value
   * throws the correct (NotSerializableException) exception.
   */
  public void testNotSerializableException() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);

    vm0.invoke(new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = getBridgeServerRegionAttributes(null,null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);

          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      });
    final int port =
      vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(vm0.getHost());

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            getLonerSystem();
            getCache();
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.LOCAL);
            factory.setConcurrencyChecksEnabled(false);

            BridgeTestCase.configureConnectionPool(factory,host0,port,-1,true,-1,-1, null);
            createRegion(name, factory.create());
          }
        };
    vm1.invoke(create);

    vm1.invoke(new CacheSerializableRunnable("Attempt to create a non-serializable value") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          try {
            region.create(new Integer(1), new BridgeLoaderTestNonSerializable());
            fail("Should not have been able to create a BridgeLoaderTestNonSerializable");
          } catch (Exception e) {
            if (!(e.getCause() instanceof java.io.NotSerializableException))
              fail("Unexpected exception while creating a non-serializable value " + e);
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Attempt to put a non-serializable value") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          try {
            region.put(new Integer(1), new BridgeLoaderTestNonSerializable());
            fail("Should not have been able to put a BridgeLoaderTestNonSerializable");
          } catch (Exception e) {
            if (!(e.getCause() instanceof java.io.NotSerializableException))
              fail("Unexpected exception while putting a non-serializable value " + e);
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Attempt to get a non-serializable key") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          try {
            region.get(new BridgeLoaderTestNonSerializable());
            fail("Should not have been able to get a BridgeLoaderTestNonSerializable");
          } catch (Exception e) {
            if (!(e.getCause() instanceof java.io.NotSerializableException))
              fail("Unexpected exception while getting a non-serializable key " + e);
          }
        }
      });

  SerializableRunnable close =
    new CacheSerializableRunnable("Close BridgeWriter") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(name);
          region.localDestroyRegion();
        }
    };

    vm1.invoke(close);

    vm0.invoke(new SerializableRunnable("Stop CacheServer") {
          public void run() {
            stopBridgeServer(getCache());
          }
    });
  }

  protected class BridgeLoaderTestNonSerializable
  {
    protected BridgeLoaderTestNonSerializable()
    {
    }
  }

  /**
   * Tests 'notify-all' client updates. This test verifies that:
   * - only invalidates are sent as part of the 'notify-all' mode
   *   of client updates
   * - originators of updates are not sent invalidates
   * - non-originators of updates are sent invalidates
   * - multiple invalidates are not sent for the same update
   */
  public void testNotifyAllUpdates() throws CacheException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    disconnectAllFromDS();

    // Create the cache servers with distributed, mirrored region
    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          CacheLoader cl = new CacheLoader() {
            public Object load(LoaderHelper helper) {
              return helper.getKey();
            }
            public void close() {

            }
          };
          AttributesFactory factory = getBridgeServerMirroredAckRegionAttributes(cl, null);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }

        }
      };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);
    vm1.invoke(createServer);

    // Create cache server clients
    final int numberOfKeys = 10;
    final String host0 = getServerHostName(host);
    final int vm0Port = vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    final int vm1Port = vm1.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    SerializableRunnable createClient =
      new CacheSerializableRunnable("Create Cache Server Client") {
        public void run2() throws CacheException {
          // reset all static listener variables in case this is being rerun in a subclass
          numberOfAfterInvalidates = 0;
          numberOfAfterCreates  = 0;
          numberOfAfterUpdates = 0;
          getLonerSystem();
          // create the region
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          // create bridge writer
          BridgeTestCase.configureConnectionPool(factory,host0,vm0Port,vm1Port,true,-1,-1, null);
         Region rgn = createRegion(name, factory.create());
        }
      };
    getSystem().getLogWriter().info("before create client");
    vm2.invoke(createClient);
    vm3.invoke(createClient);

    // Initialize each client with entries (so that afterInvalidate is called)
    SerializableRunnable initializeClient =
      new CacheSerializableRunnable("Initialize Client") {
      public void run2() throws CacheException {
        numberOfAfterInvalidates = 0;
        numberOfAfterCreates = 0;
        numberOfAfterUpdates = 0;
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        for (int i=0; i<numberOfKeys; i++) {
          assertEquals("key-"+i, region.get("key-"+i));
        }
      }
    };
    getSystem().getLogWriter().info("before initialize client");
    vm2.invoke(initializeClient);
    vm3.invoke(initializeClient);

    // Add a CacheListener to both vm2 and vm3
    vm2.invoke(new CacheSerializableRunnable("Add CacheListener 1") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        CacheListener listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent e) {
            numberOfAfterCreates++;
            getLogWriter().info("vm2 numberOfAfterCreates: " + numberOfAfterCreates);
          }
          public void afterUpdate(EntryEvent e) {
            numberOfAfterUpdates++;
            getLogWriter().info("vm2 numberOfAfterUpdates: " + numberOfAfterUpdates);
          }
          public void afterInvalidate(EntryEvent e) {
            numberOfAfterInvalidates++;
            getLogWriter().info("vm2 numberOfAfterInvalidates: " + numberOfAfterInvalidates);
          }
        };
        region.getAttributesMutator().setCacheListener(listener);
        region.registerInterestRegex(".*", false, false);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Add CacheListener 2") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        CacheListener listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent e) {
            numberOfAfterCreates++;
            //getLogWriter().info("vm3 numberOfAfterCreates: " + numberOfAfterCreates);
          }
          public void afterUpdate(EntryEvent e) {
            numberOfAfterUpdates++;
            //getLogWriter().info("vm3 numberOfAfterUpdates: " + numberOfAfterUpdates);
          }
          public void afterInvalidate(EntryEvent e) {
            numberOfAfterInvalidates++;
            //getLogWriter().info("vm3 numberOfAfterInvalidates: " + numberOfAfterInvalidates);
          }
        };
        region.getAttributesMutator().setCacheListener(listener);
        region.registerInterestRegex(".*", false, false);
      }
    });
    
    pause(3000);

    getSystem().getLogWriter().info("before puts");
    // Use vm2 to put new values
    // This should cause 10 afterUpdates to vm2 and 10 afterInvalidates to vm3
    vm2.invoke(new CacheSerializableRunnable("Put New Values") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          for (int i=0; i<10; i++) {
            region.put("key-"+i, "key-"+i);
          }
        }
      });
    getSystem().getLogWriter().info("after puts");

    // Wait to make sure all the updates are received
    pause(1000);

    int vm2AfterCreates = (int) vm2.invokeLong(ConnectionPoolDUnitTest.class, "getNumberOfAfterCreates");
    int vm2AfterUpdates = (int) vm2.invokeLong(ConnectionPoolDUnitTest.class, "getNumberOfAfterUpdates");
    int vm2AfterInvalidates = (int) vm2.invokeLong(ConnectionPoolDUnitTest.class, "getNumberOfAfterInvalidates");
    int vm3AfterCreates = (int) vm3.invokeLong(ConnectionPoolDUnitTest.class, "getNumberOfAfterCreates");
    int vm3AfterUpdates = (int) vm3.invokeLong(ConnectionPoolDUnitTest.class, "getNumberOfAfterUpdates");
    int vm3AfterInvalidates = (int) vm3.invokeLong(ConnectionPoolDUnitTest.class, "getNumberOfAfterInvalidates");
    getLogWriter().info("vm2AfterCreates: " + vm2AfterCreates);
    getLogWriter().info("vm2AfterUpdates: " + vm2AfterUpdates);
    getLogWriter().info("vm2AfterInvalidates: " + vm2AfterInvalidates);
    getLogWriter().info("vm3AfterCreates: " + vm3AfterCreates);
    getLogWriter().info("vm3AfterUpdates: " + vm3AfterUpdates);
    getLogWriter().info("vm3AfterInvalidates: " + vm3AfterInvalidates);

    assertTrue("VM2 should not have received any afterCreate messages", vm2AfterCreates==0);
    assertTrue("VM2 should not have received any afterInvalidate messages", vm2AfterInvalidates==0);
    assertTrue("VM2 received " + vm2AfterUpdates + " afterUpdate messages. It should have received " + numberOfKeys, vm2AfterUpdates==numberOfKeys);

    assertTrue("VM3 should not have received any afterCreate messages", vm3AfterCreates==0);
    assertTrue("VM3 should not have received any afterUpdate messages", vm3AfterUpdates==0);
    assertTrue("VM3 received " + vm3AfterInvalidates + " afterInvalidate messages. It should have received " + numberOfKeys, vm3AfterInvalidates==numberOfKeys);
  }
  
  /**
   * Test that the "notify by subscription" attribute is unique for each BridgeServer and Gateway
   * @throws Exception
   */
  /*
  public void testNotifyBySubscriptionIsolation() throws Exception {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    final VM server = host.getVM(3);
    final VM client1 = host.getVM(1);
    final VM client2 = host.getVM(2);

    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final int bs1Port = ports[0];
    final int bs2Port = ports[1];
    final int gwPort = ports[2];

    final String key1 = "key1-" + name;
    final String val1 = "val1-" + name;
    final String key2 = "key2-" + name;
    final String val2 = "val2-" + name;

    try {
      server.invoke(new CacheSerializableRunnable("Setup BridgeServers and Gateway") {
        public void run2() throws CacheException
        {
          Cache cache = getCache();

          try {

            // Create a gateway (which sets notify-by-subscription to true)
            cache.setGatewayHub(name, gwPort).start();

            // Start the server that does not have notify-by-subscription (server2)
            CacheServer bridge2 = cache.addCacheServer();
            bridge2.setPort(bs2Port);
            bridge2.setNotifyBySubscription(false);
            String[] noNotifyGroup = {"noNotifyGroup"};
            bridge2.setGroups(noNotifyGroup);
            bridge2.start();
            assertFalse(bridge2.getNotifyBySubscription());
            {
              BridgeServerImpl bsi = (BridgeServerImpl) bridge2;
              AcceptorImpl aci = bsi.getAcceptor();
              
              //assertFalse(aci.getCacheClientNotifier().getNotifyBySubscription());
            }

            // Start the server that DOES have notify-by-subscription (server1)
            CacheServer bridge1 = cache.addCacheServer();
            bridge1.setPort(bs1Port);
            bridge1.setNotifyBySubscription(true);
            String[] notifyGroup = {"notifyGroup"};
            bridge1.setGroups(notifyGroup);
            bridge1.start();
            assertTrue(bridge1.getNotifyBySubscription());
            {
              BridgeServerImpl bsi = (BridgeServerImpl) bridge1;
              AcceptorImpl aci = bsi.getAcceptor();
              assertTrue(aci.getCacheClientNotifier().getNotifyBySubscription());
            }

          } catch (IOException ioe) {
            fail("Setup of BridgeServer test " + name + " failed", ioe );
          }

          Region r = createRootRegion(name, getRegionAttributes());
          r.put(key1, val1);
        }
      });

      client1.invoke(new CacheSerializableRunnable("Test client1 to server with true notify-by-subscription") {
        public void run2() throws CacheException
        {
          createLonerDS();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          BridgeTestCase.configureConnectionPool(factory,getServerHostName(host),bs1Port,-1,true,-1,-1, "notifyGroup");
          factory.setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
          Region r = createRootRegion(name, factory.create());
          assertNull(r.getEntry(key1));
          r.registerInterest(key1);
          assertNotNull(r.getEntry(key1));
          assertEquals(val1, r.getEntry(key1).getValue());
          r.registerInterest(key2);
          assertNull(r.getEntry(key2));
        }
      });

      client2.invoke(new CacheSerializableRunnable("Test client2 to server with false notify-by-subscription") {
        public void run2() throws CacheException
        {
          createLonerDS();
          AttributesFactory factory = new AttributesFactory();
          BridgeTestCase.configureConnectionPool(factory,getServerHostName(host),bs2Port,-1,true,-1,-1, "noNotifyGroup");
          
          factory.setScope(Scope.LOCAL);
          factory.setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
          Region r = createRootRegion(name, factory.create());
          assertNull(r.getEntry(key1));
          assertEquals(val1, r.get(key1));
          assertNull(r.getEntry(key2));
          r.registerInterest(key2);
          assertNull(r.getEntry(key2));
        }
      });

      server.invoke(new CacheSerializableRunnable("Update server with new values for client notification") {
        public void run2() throws CacheException
        {
          Region r = getRootRegion(name);
          assertNotNull(r);
          r.put(key2, val2); // Create a new entry
          r.put(key1, val2); // Change the first entry
        }
      });

      client1.invoke(new CacheSerializableRunnable("Test update from to server with true notify-by-subscription") {
        public void run2() throws CacheException
        {
          Region r = getRootRegion(name);
          assertNotNull(r);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) r.getAttributes().getCacheListener();

          ctl.waitForUpdated(key1);
          assertNotNull(r.getEntry(key1));
          assertEquals(val2, r.getEntry(key1).getValue()); // new value should have been pushed

          ctl.waitForCreated(key2);
          assertNotNull(r.getEntry(key2)); // new entry should have been pushed
          assertEquals(val2, r.getEntry(key2).getValue());
        }
      });

      client2.invoke(new CacheSerializableRunnable("Test update from server with false notify-by-subscription") {
        public void run2() throws CacheException
        {
          Region r = getRootRegion(name);
          assertNotNull(r);
          CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) r.getAttributes().getCacheListener();
          ctl.waitForInvalidated(key1);
          assertNotNull(r.getEntry(key1));
          assertNull(r.getEntry(key1).getValue()); // Invalidate should have been pushed
          assertEquals(val2, r.get(key1)); // New value should be fetched

          assertNull(r.getEntry(key2));
          // assertNull(r.getEntry(key2).getValue());
          assertEquals(val2, r.get(key2)); // New entry should be fetched
        }
      });
      tearDown();
    } finally {
//      HashSet destroyedRoots = new HashSet();
      try {
        client1.invoke(CacheTestCase.class, "remoteTearDown");
        client1.invoke(DistributedTestCase.class, "disconnectFromDS");
      } finally {
        client2.invoke(CacheTestCase.class, "remoteTearDown");
        client2.invoke(DistributedTestCase.class, "disconnectFromDS");
      }
    }
  }
  */

  
  
  // disabled - per Sudhir we don't support multiple bridges in the same VM
//  public void test2BridgeServersWithDiffGroupsInSameVM() throws Exception {
//    final String name = this.getName();
//    final Host host = Host.getHost(0);
//    final VM server = host.getVM(3);
//    final VM client1 = host.getVM(1);
//    final VM client2 = host.getVM(2);
//
//    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
//    final int bs1Port = ports[0];
//    final int bs2Port = ports[1];
//
//    try {
//      server.invoke(new CacheSerializableRunnable("Setup BridgeServers and Gateway") {
//        public void run2() throws CacheException
//        {
//          Cache cache = getCache();
//
//          try {
//
//            // Start server in group 1
//            CacheServer bridge1 = cache.addCacheServer();
//            bridge1.setPort(bs1Port);
//            String[] group1 = {"zGroup1"};
//            bridge1.setGroups(group1);
//            bridge1.start();
//
//            // start server in group 2
//            CacheServer bridge2 = cache.addCacheServer();
//            bridge2.setPort(bs2Port);
//            bridge2.setNotifyBySubscription(true);
//            String[] group2 = {"zGroup2"};
//            bridge2.setGroups(group2);
//            bridge2.start();
//            getLogWriter().info("zGroup1 port should be "+bs1Port+" zGroup2 port should be "+bs2Port);
//          } catch (IOException ioe) {
//            fail("Setup of BridgeServer test " + name + " failed", ioe );
//          }
//
//          createRootRegion(name, getRegionAttributes());
//        }
//      });
//
//      client1.invoke(new CacheSerializableRunnable("Test client1 to zGroup2") {
//        public void run2() throws CacheException
//        {
//          createLonerDS();
//          AttributesFactory factory = new AttributesFactory();
//          factory.setScope(Scope.LOCAL);
//          BridgeTestCase.configureConnectionPool(factory,getServerHostName(host),bs1Port,-1,true,-1,-1, "zGroup2");
//          Region r = createRootRegion(name, factory.create());
//          r.registerInterest("whatever");
//        }
//      });
//
//      client2.invoke(new CacheSerializableRunnable("Test client2 to zGroup1") {
//        public void run2() throws CacheException
//        {
//          createLonerDS();
//          AttributesFactory factory = new AttributesFactory();
//          BridgeTestCase.configureConnectionPool(factory,getServerHostName(host),bs2Port,-1,true,-1,-1, "zGroup1");
//          
//          factory.setScope(Scope.LOCAL);
//          Region r = createRootRegion(name, factory.create());
//          r.registerInterest("whatever");
//        }
//      });
//
//      tearDown();
//    } finally {
//      try {
//        client1.invoke(CacheTestCase.class, "remoteTearDown");
//        client1.invoke(DistributedTestCase.class, "disconnectFromDS");
//      } finally {
//        client2.invoke(CacheTestCase.class, "remoteTearDown");
//        client2.invoke(DistributedTestCase.class, "disconnectFromDS");
//      }
//    }
//  }
  
  public static class  DelayListener extends CacheListenerAdapter {
    private final int delay;
    public DelayListener(int delay) {
      this.delay = delay;
    }
    private void delay() {
      try { Thread.sleep(this.delay); } catch (InterruptedException ignore) {fail("interrupted");}
    }
    public void afterCreate(EntryEvent event) {
      delay();
    }

    public void afterDestroy(EntryEvent event) {
      delay();
    }

    public void afterInvalidate(EntryEvent event) {
      delay();
    }

    public void afterRegionDestroy(RegionEvent event) {
      delay();
    }
  
    public void afterRegionCreate(RegionEvent event) {
      delay();
    }
  
    public void afterRegionInvalidate(RegionEvent event) {
      delay();
    }

    public void afterUpdate(EntryEvent event) {
      delay();
    }

    public void afterRegionClear(RegionEvent event) {
      delay();
    }

    public void afterRegionLive(RegionEvent event) {
      delay();
    }
  }
  /**
   * Make sure a tx done in a server on an empty region gets sent
   * to clients who have registered interest.
   */
  public void testBug39526part1() throws CacheException, InterruptedException {
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Create the cache servers with distributed, empty region
    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.EMPTY);
          factory.setConcurrencyChecksEnabled(false);
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);

    // Create cache server client
    final String host0 = getServerHostName(host);
    final int vm0Port = vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    SerializableRunnable createClient =
      new CacheSerializableRunnable("Create Cache Server Client") {
        public void run2() throws CacheException {
          getLonerSystem();
          // create the region
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          // create bridge writer
          BridgeTestCase.configureConnectionPool(factory,host0,vm0Port,-1,true,-1,-1, null);
          createRegion(name, factory.create());
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.registerInterestRegex(".*");
        }
      };
    getSystem().getLogWriter().info("before create client");
    vm1.invoke(createClient);

    // now do a tx in the server
    SerializableRunnable doServerTx =
      new CacheSerializableRunnable("doServerTx") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          Cache cache = getCache();
          CacheTransactionManager txmgr = cache.getCacheTransactionManager();
          txmgr.begin();
          try {
            region.put("k1", "v1");
            region.put("k2", "v2");
            region.put("k3", "v3");
          } finally {
            txmgr.commit();
          }
        }
      };
    getSystem().getLogWriter().info("before doServerTx");
    vm0.invoke(doServerTx);

    // now verify that the client receives the committed data
    SerializableRunnable validateClient =
      new CacheSerializableRunnable("Validate Cache Server Client") {
        public void run2() throws CacheException {
          final LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // wait for a while for us to have the correct number of entries
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return region.size() == 3;
            }
            public String description() {
              return "waiting for region to be size 3";
            }
          };
          DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
//          assertEquals(3, region.size());
          assertTrue(region.containsKey("k1"));
          assertTrue(region.containsKey("k2"));
          assertTrue(region.containsKey("k3"));
          assertEquals("v1", region.getEntry("k1").getValue());
          assertEquals("v2", region.getEntry("k2").getValue());
          assertEquals("v3", region.getEntry("k3").getValue());
        }
      };
    getSystem().getLogWriter().info("before confirmCommitOnClient");
    vm1.invoke(validateClient);
  }
  /**
   * Now confirm that a tx done in a peer of a server (the server having
   * an empty region and wanting all events) sends the tx to its clients
   */
  public void testBug39526part2() throws CacheException, InterruptedException {
    disconnectAllFromDS();
    final String name = this.getName();
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    // Create the cache servers with distributed, empty region
    SerializableRunnable createServer =
      new CacheSerializableRunnable("Create Cache Server") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setConcurrencyChecksEnabled(false);
          factory.setDataPolicy(DataPolicy.EMPTY);
          factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
          createRegion(name, factory.create());
          //pause(1000);
          try {
            startBridgeServer(0);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      };
    getSystem().getLogWriter().info("before create server");
    vm0.invoke(createServer);

    // Create cache server client
    final String host0 = getServerHostName(host);
    final int vm0Port = vm0.invokeInt(ConnectionPoolDUnitTest.class, "getCacheServerPort");
    SerializableRunnable createClient =
      new CacheSerializableRunnable("Create Cache Server Client") {
        public void run2() throws CacheException {
          getLonerSystem();
          // create the region
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          factory.setConcurrencyChecksEnabled(false);
          // create bridge writer
          BridgeTestCase.configureConnectionPool(factory,host0,vm0Port,-1,true,-1,-1, null);
          createRegion(name, factory.create());
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          region.registerInterestRegex(".*");
        }
      };
    getSystem().getLogWriter().info("before create client");
    vm1.invoke(createClient);

    SerializableRunnable createServerPeer =
      new CacheSerializableRunnable("Create Server Peer") {
        public void run2() throws CacheException {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.EMPTY);
          factory.setConcurrencyChecksEnabled(false);
          createRegion(name, factory.create());
        }
      };
    getSystem().getLogWriter().info("before create server peer");
    vm2.invoke(createServerPeer);

    // now do a tx in the server
    SerializableRunnable doServerTx =
      new CacheSerializableRunnable("doServerTx") {
        public void run2() throws CacheException {
          LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          Cache cache = getCache();
          CacheTransactionManager txmgr = cache.getCacheTransactionManager();
          txmgr.begin();
          try {
            region.put("k1", "v1");
            region.put("k2", "v2");
            region.put("k3", "v3");
          } finally {
            txmgr.commit();
          }
        }
      };
    getSystem().getLogWriter().info("before doServerTx");
    vm2.invoke(doServerTx);

    // @todo verify server received it but to do this need a listener in
    // the server
    
    // now verify that the client receives the committed data
    SerializableRunnable validateClient =
      new CacheSerializableRunnable("Validate Cache Server Client") {
        public void run2() throws CacheException {
          final LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
          // wait for a while for us to have the correct number of entries
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return region.size() == 3;
            }
            public String description() {
              return "waiting for region to be size 3";
            }
          };
          DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
//          assertEquals(3, region.size());
          assertTrue(region.containsKey("k1"));
          assertTrue(region.containsKey("k2"));
          assertTrue(region.containsKey("k3"));
          assertEquals("v1", region.getEntry("k1").getValue());
          assertEquals("v2", region.getEntry("k2").getValue());
          assertEquals("v3", region.getEntry("k3").getValue());
        }
      };
    getSystem().getLogWriter().info("before confirmCommitOnClient");
    vm1.invoke(validateClient);
    disconnectAllFromDS();
  }
}


