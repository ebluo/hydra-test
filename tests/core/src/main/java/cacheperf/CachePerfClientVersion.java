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

package cacheperf;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionRegionHelper;

import hydra.CacheHelper;

/**
 * Version support for cacheperf.CachePerfClient.
 */
public class CachePerfClientVersion {

  /**
   * Precreate buckets and ensure primaries are balanced.
   */
  public static void assignBucketsToPartitions(Region r)
  throws InterruptedException {
    if (r.getAttributes().getDataPolicy().withPartitioning()) {
      PartitionRegionHelper.assignBucketsToPartitions(r);
      RebalanceFactory rf = CacheHelper.getCache().getResourceManager()
                                       .createRebalanceFactory();
      RebalanceOperation ro = rf.start();
      RebalanceResults results = ro.getResults(); // blocking call
    }
  }
}
