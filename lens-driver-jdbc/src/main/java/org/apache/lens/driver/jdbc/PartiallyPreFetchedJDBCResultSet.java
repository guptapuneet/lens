package org.apache.lens.driver.jdbc;

import java.sql.ResultSet;

import org.apache.lens.driver.jdbc.JDBCDriver.QueryResult;
import org.apache.lens.server.api.error.LensException;

public class PartiallyPreFetchedJDBCResultSet extends JDBCResultSet {

  public PartiallyPreFetchedJDBCResultSet(QueryResult queryResult, ResultSet resultSet, boolean closeAfterFetch)
      throws LensException {
    super(queryResult, resultSet, closeAfterFetch);
  }

}
