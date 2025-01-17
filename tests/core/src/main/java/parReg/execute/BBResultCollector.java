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
package parReg.execute;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import parReg.ParRegBB;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

public class BBResultCollector implements ResultCollector {

  ArrayList result = new ArrayList();  

  public void addResult(DistributedMember memberID,
      Object nodeList) {
    result.add(nodeList);
    ParRegBB.getBB().getSharedCounters().incrementAndRead(
        ParRegBB.resultSenderCounter);
  }

  public void endResults() {
    
  }

  public Object getResult() throws FunctionException {
    return result;
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    return result;
  }
  
  public synchronized void clearResults() {
    result.clear();
  }
}
