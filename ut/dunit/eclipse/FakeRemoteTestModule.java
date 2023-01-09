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
package dunit.eclipse;

import hydra.MethExecutor;
import hydra.MethExecutorResult;
import hydra.RemoteTestModuleIF;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import batterytest.greplogs.ExpectedStrings;
import batterytest.greplogs.LogConsumer;
import batterytest.greplogs.SuspectGrepOutputStream;

import com.gemstone.gemfire.LogWriter;

/**
 * @author dsmith
 *
 */
public class FakeRemoteTestModule extends UnicastRemoteObject implements RemoteTestModuleIF {
  
  private LogWriter log;
 
  
  public FakeRemoteTestModule(LogWriter log) throws RemoteException {
    super();
    this.log = log;
  }

  /** 
   * Called remotely by the master controller to cause the client to execute 
   * the instance method on the object.  Does this synchronously (does not spawn
   * a thread).  This method is used by the unit test framework, dunit.
   *
   * @param obj the object to execute the method on
   * @param methodName the name of the method to execute
   * @return the result of method execution
   */ 
   public MethExecutorResult executeMethodOnObject( Object obj, String methodName ) {
     String name = obj.getClass().getName() + "." + methodName + 
       " on object: " + obj;
     log.info("Received method: " + name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = MethExecutor.executeObject( obj, methodName );
     long delta = System.currentTimeMillis() - start;
     log.info( "Got result: " + result.toString().trim()  + " from " +
               name + " (took " + delta + " ms)");
     return result;
   }

   /**
    * Executes a given instance method on a given object with the given
    * arguments. 
    */
   public MethExecutorResult executeMethodOnObject(Object obj,
                                                   String methodName,
                                                   Object[] args) {
     String name = obj.getClass().getName() + "." + methodName + 
              (args != null ? " with " + args.length + " args": "") +
       " on object: " + obj;
     log.info("Received method: " + name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = 
       MethExecutor.executeObject(obj, methodName, args);
     long delta = System.currentTimeMillis() - start;
     log.info( "Got result: " + result.toString() + " from " + name + 
               " (took " + delta + " ms)");
     return result;
   }

  /** 
   * Called remotely by the master controller to cause the client to execute 
   * the method on the class.  Does this synchronously (does not spawn a thread).
   * This method is used by the unit test framework, dunit.
   *
   * @param className the name of the class execute
   * @param methodName the name of the method to execute
   * @return the result of method execution
   */ 
   public MethExecutorResult executeMethodOnClass( String className, String methodName ) {
     String name = className + "." + methodName;
     log.info("Received method: " +  name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = MethExecutor.execute( className, methodName );
     long delta = System.currentTimeMillis() - start;
     log.info( "Got result: " + result.toString() + " from " + name + 
               " (took " + delta + " ms)");
     
     return result;
   }

   /**
    * Executes a given static method in a given class with the given
    * arguments. 
    */
   public MethExecutorResult executeMethodOnClass(String className,
                                                  String methodName,
                                                  Object[] args) {
     String name = className + "." + methodName + 
       (args != null ? " with " + args.length + " args": "");
     log.info("Received method: " + name);
     long start = System.currentTimeMillis();
     MethExecutorResult result = 
       MethExecutor.execute(className, methodName, args);
     long delta = System.currentTimeMillis() - start;
     log.info( "Got result: " + result.toString() + " from " + name +
               " (took " + delta + " ms)");
     return result;
   }

  public void executeTask(int tsid, int type, int index) throws RemoteException {
    throw new UnsupportedOperationException();
    
  }
  
  public void runShutdownHook() throws RemoteException {
    
  }

  public void notifyDynamicActionComplete(int actionId) throws RemoteException {
    throw new UnsupportedOperationException();
    
  }

  public void disconnectVM()
  throws RemoteException {
  }
  
  public void shutDownVM(boolean disconnect, boolean runShutdownHook)
      throws RemoteException {
  }

}
