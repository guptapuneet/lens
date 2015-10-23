/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.driver;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import lombok.Getter;

/**
 * Abstract class for Lens Driver Implementations. Provides default
 * implementations and some utility methods for drivers
 */
public abstract class AbstractLensDriver implements LensDriver {
  /*
   * Type of Driver ( Example hive,jdbc)
   */
  @Getter
  private String driverType = null;

  /*
   * Name of Driver
   */
  @Getter
  private String driverName = null;

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    if (StringUtils.isBlank(driverType) || StringUtils.isBlank(driverName)) {
      throw new LensException("Driver Type or Name is empty");
    }
    this.driverType = driverType;
    this.driverName = driverName;
  }

  /**
   * Gets the path for the driver resource in the system. This is a utility
   * method that can be used by sub classes to build resource paths.
   *
   * @param resourceName
   * @return
   */
  public String getDriverResourcePath(String resourceName) {
    return new StringBuilder(LensConfConstants.DRIVERS_BASE_DIR).append('/').append(getFullyQualifiedName())
        .append('/').append(resourceName).toString();
  }

  @Override
  public String getFullyQualifiedName() {
    return new StringBuilder(driverType).append('/').append(driverName).toString();
  }
}
