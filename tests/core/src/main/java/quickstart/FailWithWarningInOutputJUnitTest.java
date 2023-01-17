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
package quickstart;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.LocalLogWriter;
import org.apache.geode.internal.LogWriterImpl;

/**
 * Verifies that quickstart test output containing an unexpected warning message
 * will fail with that warning message as the failure message.
 * 
 * @author Kirk Lund
 */
public class FailWithWarningInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  public FailWithWarningInOutputJUnitTest() {
    super(FailWithWarningInOutputJUnitTest.class.getSimpleName());
  }
  
  @Override
  String problem() {
    return "[warning 2014/03/13 23:13:41.629 PDT Name <Some Thread> tid=0x2a] Description of a problem.";
  }
  
  @Override
  void outputProblem(String message) {
    LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    logWriter.warning(message);
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithWarningInOutputJUnitTest().execute();
  }
}
