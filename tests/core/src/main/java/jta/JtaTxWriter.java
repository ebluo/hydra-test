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
package jta; 

import java.util.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.*;

import util.*;
import hydra.*;

/**
 * A writer for handling transaction related events (beforeCommit).  
 * Note that RegionEvents are not transactional and are not handled 
 * by this writer.  This is because they occur at the time of execution 
 * rather than at commit time.
 *
 * @author lhughes
 * @see TransactionWriter
 */
public class JtaTxWriter extends util.AbstractWriter implements TransactionWriter {

  /** Called after successful conflict checking (in GemFire) and prior to commit
   *  The TxWriter simply commits the work done in the dataBase earlier by 
   *  the CacheWriter.
   *
   *  @param txEvent the TransactionEvent
   */
  public void beforeCommit(TransactionEvent txEvent) throws TransactionWriterException {
    logTxEvent("beforeCommit", txEvent);
   
    // change to randomly throw TransactionWriterExceptions
    int i = TestConfig.tab().getRandGen().nextInt(1,100);
    if (i < 10) {
      Log.getLogWriter().info("JtaTxWriter intentionally throwing TransactionWriterException");
      throw new TestException("TransactionWriter intentionally throws TransactionWriterException");
    } else if (i < 20) {
      Log.getLogWriter().info("JtaTxWriter intentionally throwing NPE (RuntimeException)");
      throw new NullPointerException("TransactionWriter intentionally throws NPE");
    }
  }
  
  /** Utility method to log an Exception, place it into a wellknown location in the 
   *  EventBB and throw an exception.
   *
   *  @param errString string to be logged, placed on EventBB and include in
   *         Exception
   *  @throws TestException containing the given string
   *  @see TestHelper.checkForEventError
   */
  protected void throwException(String errStr) throws TransactionWriterException {
    StringBuffer qualifiedErrStr = new StringBuffer();
    qualifiedErrStr.append("Exception reported in " + RemoteTestModule.getMyClientName() + "\n");
    qualifiedErrStr.append(errStr);
    errStr = qualifiedErrStr.toString();
       
    hydra.blackboard.SharedMap aMap = JtaBB.getBB().getSharedMap();
    aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());         
    Log.getLogWriter().info(errStr);
    throw new TransactionWriterException(errStr);
  }

  /**  Called when the region containing this callback is destroyed, when the 
   *   cache is closed, or when a callback is removed from a region using an
   *   <code>AttributesMutator</code>.
   */
  public void close() {
    Log.getLogWriter().info("JtaTxWriter.close()");
  }
}

