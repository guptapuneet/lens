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

package org.apache.lens.client.model;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.client.LensStatement;

import lombok.extern.slf4j.Slf4j;

/**
 * This class can be used to create Proxy Lens Query objects. The Proxy objects support lazy initialization
 * of members of this class given a query handle and LensStatement.
 *
 * Note: In most cases the query handle information should suffice which is available locally, and only in a few cases
 * like {@link org.apache.lens.client.LensClient.LensClientResultSetWithStats}, extra information needs to be fetched
 * from Lens Server.
 */
@Slf4j
public class ProxyLensQuery extends LensQuery {

  private LensStatement statement;
  private QueryHandle queryHandle;
  private boolean isFullyInitialized;
  private LensQuery actualLensQuery;

  public ProxyLensQuery(LensStatement statement, QueryHandle queryHandle) {
    this.statement = statement;
    this.queryHandle = queryHandle;
  }

  @Override
  public QueryHandle getQueryHandle() {
    return this.queryHandle;
  }

  @Override
  public String getSubmittedUser() {
    return this.statement.getUser();
  }

  @Override
  public String getUserQuery() {
    lazyInit();
    return actualLensQuery.getUserQuery();
  }

  @Override
  public Priority getPriority() {
    lazyInit();
    return actualLensQuery.getPriority();
  }

  @Override
  public boolean isPersistent() {
    lazyInit();
    return actualLensQuery.isPersistent();
  }

  @Override
  public String getSelectedDriverName() {
    lazyInit();
    return actualLensQuery.getSelectedDriverName();
  }

  @Override
  public String getDriverQuery() {
    lazyInit();
    return actualLensQuery.getDriverQuery();
  }

  @Override
  public QueryStatus getStatus() {
    lazyInit();
    return actualLensQuery.getStatus();
  }

  @Override
  public String getResultSetPath() {
    lazyInit();
    return actualLensQuery.getResultSetPath();
  }

  @Override
  public String getDriverOpHandle() {
    lazyInit();
    return actualLensQuery.getDriverOpHandle();
  }

  @Override
  public LensConf getQueryConf() {
    lazyInit();
    return actualLensQuery.getQueryConf();
  }

  @Override
  public long getSubmissionTime() {
    lazyInit();
    return actualLensQuery.getSubmissionTime();
  }

  @Override
  public long getLaunchTime() {
    lazyInit();
    return actualLensQuery.getLaunchTime();
  }

  @Override
  public long getDriverStartTime() {
    lazyInit();
    return actualLensQuery.getDriverStartTime();
  }

  @Override
  public long getDriverFinishTime() {
    lazyInit();
    return actualLensQuery.getDriverFinishTime();
  }

  @Override
  public long getFinishTime() {
    lazyInit();
    return actualLensQuery.getFinishTime();
  }

  @Override
  public long getClosedTime() {
    lazyInit();
    return actualLensQuery.getClosedTime();
  }

  @Override
  public String getQueryName() {
    lazyInit();
    return actualLensQuery.getQueryName();
  }

  @Override
  public Integer getErrorCode() {
    lazyInit();
    return actualLensQuery.getErrorCode();
  }

  @Override
  public String getErrorMessage() {
    lazyInit();
    return actualLensQuery.getErrorMessage();
  }

  @Override
  public String getQueryHandleString() {
    lazyInit();
    return actualLensQuery.getQueryHandleString();
  }

  @Override
  public boolean queued() {
    lazyInit();
    return actualLensQuery.queued();
  }

  private synchronized void lazyInit() {
    if (!isFullyInitialized) {
      this.actualLensQuery = statement.getQuery(queryHandle);
      this.isFullyInitialized = true;
    }
  }
}
