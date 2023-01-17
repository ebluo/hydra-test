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
package container.app.phonedirectory.support;

import org.apache.geode.cache.EntryNotFoundException;

public class PhoneDirectoryEntryNotFoundException extends EntryNotFoundException {
  
  private static final long serialVersionUID = 11011011L;

  public PhoneDirectoryEntryNotFoundException() {
    super("No Message or Cause Provided!");
  }

  public PhoneDirectoryEntryNotFoundException(final String message) {
    super(message);
  }

  public PhoneDirectoryEntryNotFoundException(final Throwable cause) {
    super("No Message Provided!", cause);
  }

  public PhoneDirectoryEntryNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
