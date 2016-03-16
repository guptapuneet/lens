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

package org.apache.lens.cleint.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.client.LensStatement;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This class can be used to create dynamic proxy LensQuery objects. The proxy objects support lazy initialization
 * of members of LensQuery type objects given a query handle and LensStatement.
 */
@Slf4j
public class ProxyLensQuery {

  private ProxyLensQuery() {
  }

  /**
   * Gets a proxy instance of lens query which can answer getQueryHandle and getSubmittedUser locally and delegates
   * all other calls to LensQuery object which is created using and extra server call via {@link #getQuery(QueryHandle)}
   * @param statement
   * @param queryHandle
   * @return
   */
  public static LensQuery createProxy(LensStatement statement, QueryHandle queryHandle) {
    return (LensQuery)Proxy.newProxyInstance(statement.getClass().getClassLoader(), new Class[]{LensQuery.class },
        new ProxyLensQueryInvocationHandler(statement, queryHandle));
  }

  @RequiredArgsConstructor
  private static class ProxyLensQueryInvocationHandler implements InvocationHandler {
    private final LensStatement statement;
    private final QueryHandle queryHandle;
    private LensQuery lensQuery;

    // preloaded Method objects for the methods in java.lang.Object
    private static Method hashCodeMethod;
    private static Method equalsMethod;
    private static Method toStringMethod;
    static {
      try {
        hashCodeMethod = Object.class.getMethod("hashCode", null);
        equalsMethod = Object.class.getMethod("equals", new Class[] { Object.class });
        toStringMethod = Object.class.getMethod("toString", null);
      } catch (NoSuchMethodException e) {
        log.error("Error while creating ProxyLensQueryInvocationHandler", e); // This is unexpected
        throw new NoSuchMethodError(e.getMessage());
      }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Class<?> declaringClass = method.getDeclaringClass();
      String methodName = method.getName();
      if (declaringClass == Object.class) {
        if (method.equals(hashCodeMethod)) {
          return getQuery().hashCode();
        } else if (method.equals(equalsMethod)) {
          return proxyEquals(proxy, args[0]);
        } else if (method.equals(toStringMethod)) {
          return getQuery().toString();
        } else {
          throw new InternalError("unexpected Object method dispatched: " + method);
        }
      } else {
        if (methodName.equals("getQueryHandle")) {
          return queryHandle;
        } else if (methodName.equals("getSubmittedUser")) {
          return statement.getUser();
        } else {
          return LensQuery.class.getMethod(methodName, getArgTypes(args)).invoke(getQuery(), args);
        }
      }
    }

    public Class<?>[] getArgTypes(Object[] args) {
      if (null == args) {
        return null;
      }
      Class<?>[] argTypes = new Class[args.length];
      int i = 0;
      for (Object arg : args) {
        argTypes[i++] = arg.getClass();
      }
      return argTypes;
    }

    /**
     * Gets cached lens query. If nothing is cached, creates and caches it first.
     */
    private synchronized LensQuery getQuery() {
      if (lensQuery == null) {
        this.lensQuery = statement.getQuery(queryHandle);
      }
      return this.lensQuery;
    }

    protected boolean proxyEquals(Object proxy, Object other) {
      if (this == other) {
        return true;
      }

      if (other == null) {
        return false;
      }

      if (!(other instanceof LensQuery) || !(proxy instanceof LensQuery)) {
        return false;
      }

      return ((LensQuery)proxy).getQueryHandle().equals(((LensQuery)other).getQueryHandle());
    }
  }
}
