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
 * Verifies that an example test should always pass even if the output contains
 * a severe that is expected.
 * 
 * @author Kirk Lund
 */
public class PassWithExpectedSevereJUnitTest extends PassWithExpectedProblemTestCase {
  
  public PassWithExpectedSevereJUnitTest() {
    super("PassWithExpectedSevereJUnitTest");
  }
  
  @Override
  String problem() {
    return "severe";
  }
  
  @Override
  void outputProblem(String message) {
    LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    logWriter.severe(message);
  }
  
  public static void main(String[] args) throws Exception {
    new PassWithExpectedSevereJUnitTest().execute();
  }
}
