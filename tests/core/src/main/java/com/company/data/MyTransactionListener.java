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
package com.company.data;

import org.apache.geode.cache.*;

/**
 * A <code>TransactionListener</code> that is <code>Declarable</code>
 *
 * @author Mitch Thomas
 * @since 4.0
 */
public class MyTransactionListener implements TransactionListener, Declarable {

  public void afterCommit(TransactionEvent event) {}
    
  public void afterFailedCommit(TransactionEvent event) {}

  public void afterRollback(TransactionEvent event) {}

  public void init(java.util.Properties props) {}

  public void close() {}

}
