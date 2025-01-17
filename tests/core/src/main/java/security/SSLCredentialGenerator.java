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
package security;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Properties;

import org.apache.geode.security.AuthenticationFailedException;

public class SSLCredentialGenerator extends CredentialGenerator {

  private File findTrustedJKS() {
    File ssldir = new File(System.getProperty("JTESTS") + "/ssl");
    return new File(ssldir, "trusted.keystore");
  }

  private File findUntrustedJKS() {
    File ssldir = new File(System.getProperty("JTESTS") + "/ssl");
    return new File(ssldir, "untrusted.keystore");
  }

  private Properties getValidJavaSSLProperties() {
    File jks = findTrustedJKS();
    try {
      Properties props = new Properties();
      props.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.trustStorePassword", "password");
      props.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.keyStorePassword", "password");
      return props;
    }
    catch (IOException ex) {
      throw new AuthenticationFailedException(
          "SSL: Exception while opening the key store: " + ex);
    }
  }

  private Properties getInvalidJavaSSLProperties() {
    File jks = findUntrustedJKS();
    try {
      Properties props = new Properties();
      props.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.trustStorePassword", "password");
      props.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.keyStorePassword", "password");
      return props;
    }
    catch (IOException ex) {
      throw new AuthenticationFailedException(
          "SSL: Exception while opening the key store: " + ex);
    }
  }

  private Properties getSSLProperties() {
    Properties props = new Properties();
    props.setProperty("ssl-enabled", "true");
    props.setProperty("ssl-require-authentication", "true");
    props.setProperty("ssl-ciphers", "SSL_RSA_WITH_RC4_128_MD5");
    props.setProperty("ssl-protocols", "SSLv3");
    return props;
  }

  protected Properties initialize() throws IllegalArgumentException {
    this.javaProps = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  public ClassCode classCode() {
    return ClassCode.SSL;
  }

  public String getAuthInit() {
    return null;
  }

  public String getAuthenticator() {
    return null;
  }

  public Properties getValidCredentials(int index) {
    this.javaProps = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  public Properties getValidCredentials(Principal principal) {
    this.javaProps = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  public Properties getInvalidCredentials(int index) {
    this.javaProps = getInvalidJavaSSLProperties();
    return getSSLProperties();
  }

}
