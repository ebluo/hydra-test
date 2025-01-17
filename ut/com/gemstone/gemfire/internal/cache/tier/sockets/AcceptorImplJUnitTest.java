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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.IOException;
import java.net.BindException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class AcceptorImplJUnitTest extends TestCase
{

  DistributedSystem system;
  Cache cache;
  protected void setUp() throws Exception
  {
    //LogWriter log = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    super.setUp();
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME ,"0");
    //p.setProperty("log-level", getGemFireLogLevel());
    //System.getProperties().setProperty("DistributionManager.VERBOSE", "true");
    this.system = DistributedSystem.connect(p);
    this.cache = CacheFactory.create(system);
  }

  protected void tearDown() throws Exception
  {
    this.cache.close();
    this.system.disconnect();
    super.tearDown();
  }

  /*
   * Test method for 'com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl(int, int, boolean, int, Cache)'
   */
  public void foo_testConstructor() throws CacheException, IOException
  {
    AcceptorImpl a1 = null, a2 = null, a3 = null;
    try {
      final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      int port1 = freeTCPPorts[0];
      int port2 = freeTCPPorts[1];


      try {
        new AcceptorImpl(
          port1,
          null,
          false,
          BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE,
          BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          AcceptorImpl.MINIMUM_MAX_CONNECTIONS - 1,
          BridgeServer.DEFAULT_MAX_THREADS,
          BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE,0,null,null, false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        fail("Expected an IllegalArgumentExcption due to max conns < min pool size");
      } catch (IllegalArgumentException expected) {
      }

      try {
        new AcceptorImpl(
          port2,
          null,
          false,
          BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE,
          BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          0,
          BridgeServer.DEFAULT_MAX_THREADS,
          BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE,0,null,null, false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        fail("Expected an IllegalArgumentExcption due to max conns of zero");
      } catch (IllegalArgumentException expected) {
      }

      try {
        a1 = new AcceptorImpl(
          port1,
          null,
          false,
          BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE,
          BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
          BridgeServer.DEFAULT_MAX_THREADS,
          BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE,0,null,null, false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        a2 = new AcceptorImpl(
          port1,
          null,
          false,
          BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE,
          BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          this.cache,
          AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
          BridgeServer.DEFAULT_MAX_THREADS,
          BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
          BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE,0,null,null, false, Collections.EMPTY_LIST,
          CacheServer.DEFAULT_TCP_NO_DELAY);
        fail("Expecetd a BindException while attaching to the same port");
      } catch (BindException expected) {
      }

      a3 = new AcceptorImpl(
        port2,
        null,
        false,
        BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE,
        BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
        BridgeServer.DEFAULT_MAX_THREADS,
        BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
        BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE,0,null,null, false, Collections.EMPTY_LIST,
        CacheServer.DEFAULT_TCP_NO_DELAY);
      assertEquals(port2, a3.getPort());
      InternalDistributedSystem isystem = (InternalDistributedSystem) this.cache.getDistributedSystem();
      DistributionConfig config = isystem.getConfig();
      String bindAddress = config.getBindAddress();
      if (bindAddress == null || bindAddress.length() <= 0) {
        assertTrue(a3.getServerInetAddr().isAnyLocalAddress());
      }
    } finally {
      if (a1!=null)
        a1.close();
      if (a2!=null)
        a2.close();
      if (a3!=null)
        a3.close();
    }
  }

  /*
   * Test method for 'com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl.start()'
   * Since for CBB branch , the ping protocol has been modified such that the 
   * server connection thread exits after recieving the ping protocol, using
   * a non ping message to test the conenction limit 
   */
  /*public void disable_testStartAndAccept() throws IOException, ServerRefusedConnectionException
  {
    //LogWriter log = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    //Create a temporary test region
	Region temp= null;  
    AcceptorImpl ac = null;
    Socket[] clis = null;
    try{
    	temp =cache.createRegion("testRoot", new AttributesFactory().create());
    }catch(Exception ignore) {
    	
    }
    // Test getting to the limit of connection
    
    Message putMsg= new Message(3);
    try {
      
      int port2 = AvailablePort.getRandomAvailablePort( AvailablePort.SOCKET );
      ac = new AcceptorImpl(
        port2,
        null,
        false,
        BridgeServer.DEFAULT_SOCKET_BUFFER_SIZE,
        BridgeServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        this.cache,
        AcceptorImpl.MINIMUM_MAX_CONNECTIONS,
        BridgeServer.DEFAULT_MAX_THREADS,
        BridgeServer.DEFAULT_MAXIMUM_MESSAGE_COUNT,
        BridgeServer.DEFAULT_MESSAGE_TIME_TO_LIVE,null,null);
      ac.start();
      putMsg.setMessageType(MessageType.PUT);
      putMsg.setTransactionId(1);
      putMsg.setNumberOfParts(4);
      putMsg.addStringPart("testRoot");
      putMsg.addStringOrObjPart("key1");
      putMsg.addObjPart("key1", false);
      putMsg.addBytesPart(EventID.getOptimizedByteArrayForEventID(1, 1));
 
      clis = new Socket[AcceptorImpl.MINIMUM_MAX_CONNECTIONS];
      for(int i=0; i<clis.length; i++) {
        clis[i] = new Socket((String) null, port2);
        clis[i].setSoTimeout(1000);
        clis[i].getOutputStream().write(Acceptor.CLIENT_TO_SERVER);
        putMsg.setComms(clis[i], ServerConnection.allocateCommBuffer(1024));
        HandShake.getHandShake().greet(clis[i], null);
        putMsg.send();
      }

      // Test when we are over the max
      Socket oneOver = new Socket((String) null, port2);
      oneOver.setSoTimeout(3000);
      oneOver.getOutputStream().write(Acceptor.CLIENT_TO_SERVER);
      try {
        HandShake.getHandShake().greet(oneOver, null);
        fail("Expecting the server to halt accepting connects");
      } catch (SocketTimeoutException expected) {
      } finally {
        oneOver.close();
      }

      final String expecteExceptions = "Unexpected IOException||java.io.InterruptedIOException||java.io.IOException||java.net.SocketException||java.net.SocketException: Broken pipe";
      this.cache.getLogger().info("<ExpectedException action=add>" + expecteExceptions + "</ExpectedException>");
      // Test recovery of some connections using a messy close on the connection
      // (An ordered close sends a message regarding the close)
      for (int j=0; j<=1; j++) {
        clis[j].close();

        // Wait for an available ServerConnection thread
        ac.testWaitForAvailableWorkerThread();

        // Wait for server to begin accepting connections
        // this test method no longer exists ac.testWaitForAcceptorThread();
        clis[j] = new Socket((String) null, port2);
        clis[j].setSoTimeout(2000);
        clis[j].getOutputStream().write(Acceptor.CLIENT_TO_SERVER);
        putMsg.setComms(clis[j], ServerConnection.allocateCommBuffer(1024));
        HandShake.getHandShake().greet(clis[j], null);
        putMsg.send();
      }
      this.cache.getLogger().info("<ExpectedException action=remove>" + expecteExceptions + "</ExpectedException>");
    } finally {
      if (ac!=null)
        ac.close();
      if (clis!=null) {
        for(int i=0; i<clis.length; i++) {
          if (clis[i] != null)
            clis[i].close();
        }
      }
      try{
    	  temp.destroyRegion();
      }catch(Exception ignore) {
    	  
      }
    }
  }*/
  public void testNothing() {
  }
}
