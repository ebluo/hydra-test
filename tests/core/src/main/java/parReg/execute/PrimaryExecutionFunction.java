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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import util.TestException;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;

public class PrimaryExecutionFunction extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
    ArrayList<String> list = new ArrayList<String>();
    RegionFunctionContext prContext = (RegionFunctionContext)context;
    LocalDataSet localDataSet = (LocalDataSet)PartitionRegionHelper
        .getLocalDataForContext(prContext);
    PartitionedRegion pr = (PartitionedRegion)prContext.getDataSet();
    String localNodeId = InternalDistributedSystem.getAnyInstance()
        .getMemberId();

    Set localBucketSet = localDataSet.getBucketSet();
    List primaryBucketList = pr.getLocalPrimaryBucketsListTestOnly();

    if (primaryBucketList.containsAll(localBucketSet)) {
      // hydra.Log.getLogWriter().info("localBucketSet is "+localBucketSet+ "
      // primaryBucketList is "+primaryBucketList);
      list.add(localNodeId);
    }
    else {
      throw new TestException(
          "Function execution with optimizeForWrite as true operated on buckets "
              + localBucketSet + " but primary buckets on this node are "
              + primaryBucketList);
    }

    prContext.getResultSender().lastResult(list);
  }

  public String getId() {
    return "PrimaryExecutionFunction";
  }

  public boolean optimizeForWrite() {
    return true;
  }
  
  public boolean isHA() {
    return true;
  }

  public void init(Properties props) {
  }

}
