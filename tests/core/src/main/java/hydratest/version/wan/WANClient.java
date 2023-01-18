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

package hydratest.version.wan;

import org.apache.geode.cache.*;
import org.apache.geode.cache.util.*;
import org.apache.geode.internal.GemFireVersion;
import hydra.*;

public class WANClient {

  public static void openCacheTask() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
  }

  public static void createRegionTask() {
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
  }

  // public static void reportGatewayAttributesTask() {
  //   GatewayHub hub = GatewayHubHelper.getGatewayHub();
  //   if (hub == null) {
  //     throw new HydraConfigException("Gateway not configured");
  //   }
  //   else {
  //     String s = "In GemFire version " + GemFireVersion.getGemFireVersion()
  //              + ", the gateway hub and its gateways are "
  //              + GatewayHubHelper.gatewayHubToString(hub);
  //     Log.getLogWriter().info(s);
  //   }
  // }

  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }
}
