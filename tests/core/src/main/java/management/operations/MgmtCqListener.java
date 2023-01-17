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
package management.operations;

import static management.util.HydraUtil.logInfo;
import static management.util.HydraUtil.logError;
import static management.util.HydraUtil.ObjectToString;

import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;

public class MgmtCqListener implements CqListener {

  @Override
  public void close() {
    logInfo("MgmtCqListener: Query Closed !!!");
  }

  @Override
  public void onEvent(CqEvent aCqEvent) {
    logInfo("MgmtCqListener: cq event " + ObjectToString(aCqEvent));
  }

  @Override
  public void onError(CqEvent aCqEvent) {
    logError("MgmtCqListener: error on event " + ObjectToString(aCqEvent));
  }

}
