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
/**
 * 
 */
package pdx;

import org.apache.geode.cache.EntryEvent;

import hct.BridgeEventListener;

/**
 * @author lynn
 *
 */
public class PdxBridgeEventListener extends BridgeEventListener {
  
  /* (non-Javadoc)
   * @see util.AbstractListener#getOldValueStr(com.gemstone.gemfire.cache.EntryEvent)
   */
  @Override
  public String getOldValueStr(EntryEvent eEvent) {
    return PdxTest.getOldValueStr(eEvent);
  }

  /* (non-Javadoc)
   * @see util.AbstractListener#getNewValueStr(com.gemstone.gemfire.cache.EntryEvent)
   */
  @Override
  public String getNewValueStr(EntryEvent eEvent) {
    return PdxTest.getNewValueStr(eEvent);
  }

}
