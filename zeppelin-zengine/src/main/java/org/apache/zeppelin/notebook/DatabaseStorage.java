/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.notebook;


import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author a0s03ny
 * this class creates an mysql connection pool.
 */
public class DatabaseStorage{
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseStorage.class);
  private BasicDataSource basicDataSource = new BasicDataSource();
  private static ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
  private static final String USERNAME = zConf.getConfDBUsername();
  private static final String PASSWORD = zConf.getConfDBPassword();
  private static final String URL = zConf.getConfDBURL();
  private static final String DRIVER = zConf.getConfDBDriver();
  private static final int CONN_POOL_SIZE = 50;

  private DatabaseStorage(){
    basicDataSource.setDriverClassName(DRIVER);
    basicDataSource.setUrl(URL);
    basicDataSource.setUsername(USERNAME);
    basicDataSource.setPassword(PASSWORD);
    basicDataSource.setInitialSize(CONN_POOL_SIZE);
  }

  private static class DataSourceObject{
    private static final DatabaseStorage INSTANCE = new DatabaseStorage();
  }

  public static DatabaseStorage getInstance(){
    LOGGER.info("getting data base instance");
    return DataSourceObject.INSTANCE;
  }

  public BasicDataSource getBasicDataSource(){
    return basicDataSource;
  }
  public void setBasicDataSource(BasicDataSource bds){
    this.basicDataSource = bds;
  }
}
