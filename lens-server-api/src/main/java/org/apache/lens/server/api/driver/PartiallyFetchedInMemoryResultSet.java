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

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.error.LensException;

/**
 * This is a wrapper over InMemoryResultSet which pre-fetches requested number of rows in memory. All calls are 
 * delegated to the underlying InMemoryResultSet except for the calls that access pre-fetched rows.
 * 
 * This wrapper was created to support partial streaming of big result sets and complete streaming of SMALL result sets
 * along with persistence. The pre-fetched result available via {@link #getPreFetchedRows()} can be used for streaming 
 * while the persistence logic can iterate over complete result set using {@link #hasNext()} and {@link #next()}.
 * 
 * Please note that streaming and persistence can occur concurrently irrespective of the underlying InMemoryResultSet 
 * implementation.
 */
@Slf4j
public class PartiallyFetchedInMemoryResultSet extends InMemoryResultSet {

  private InMemoryResultSet inMemoryRS;
  private int reqPreFetchSize;
  private int actualPreFetchSize;
  private int cursor;
  @Getter
  private boolean isComplteleyFetched;
  @Getter
  private List<ResultRow> preFetchedRows;

  public PartiallyFetchedInMemoryResultSet(InMemoryResultSet inMemoryRS, int reqPreFetchSize) throws LensException {
    this.inMemoryRS = inMemoryRS;
    this.reqPreFetchSize = reqPreFetchSize;
    if (reqPreFetchSize <= 0) {
      throw new IllegalArgumentException("Invalid pre fetch size " + reqPreFetchSize);
    }
    preFetchRows();
  }

  private void preFetchRows() throws LensException {
    preFetchedRows = new ArrayList<ResultRow>(reqPreFetchSize);
    while(inMemoryRS.hasNext()) {
      preFetchedRows.add(inMemoryRS.next());
      if (++actualPreFetchSize >= reqPreFetchSize) {
        break;
      }
    }
    if (actualPreFetchSize < reqPreFetchSize) {
      isComplteleyFetched = true;
    }
    log.info("Pre Fetched {} rows of requested {} rows", actualPreFetchSize, reqPreFetchSize);
  }

  @Override
  public boolean seekToStart() throws LensException {
    cursor = 0;
    return inMemoryRS.seekToStart();
  }

  @Override
  public boolean hasNext() throws LensException {
    cursor++;
    if (cursor <= actualPreFetchSize) {
      return true;
    }
    return inMemoryRS.hasNext();
  }

  @Override
  public ResultRow next() throws LensException {
    if (cursor <= actualPreFetchSize) {
      return preFetchedRows.get(cursor-1);
    }
    return inMemoryRS.next();
  }

  @Override
  public void setFetchSize(int size) throws LensException {
    inMemoryRS.setFetchSize(size);
  }

  @Override
  public Integer size() throws LensException {
    return inMemoryRS.size();
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return inMemoryRS.getMetadata();
  }

}
