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
package org.apache.zeppelin.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterInfoSaving;
import org.apache.zeppelin.notebook.DatabaseStorage;
import org.apache.zeppelin.notebook.NotebookAuthorizationInfoSaving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author a0s03ny
 *
 */
public class DataBaseConfigStorage
    extends ConfigStorage
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseConfigStorage.class);
  private BasicDataSource dbConfigStorage;
  private Connection connection;

  public DataBaseConfigStorage(ZeppelinConfiguration zConf)
  {
    super(zConf);
    dbConfigStorage = DatabaseStorage.getInstance().getBasicDataSource();
  }

  @Override
  public void save(InterpreterInfoSaving settingInfos)
      throws IOException
  {
    LOGGER.info("saving interpreter settings to db");
    LOGGER.info("json file " + settingInfos.toJson());
    writetoDB("update datalake_console.zeppelin_settings set "
        + "interpreter_settings = '" + settingInfos.toJson() + "' where id = 1");
  }

  @Override
  public InterpreterInfoSaving loadInterpreterSettings()
      throws IOException
  {
    LOGGER.info("retrive interpreter settings from db");
    String json = readFromDB("select interpreter_settings from datalake_console.zeppelin_settings"
        + " where id = 1");
    return buildInterpreterInfoSaving(json);
  }

  @Override
  public void save(NotebookAuthorizationInfoSaving authorizationInfoSaving)
      throws IOException
  {
    LOGGER.info("Save notebook authorization to DB");
    writetoDB("update datalake_console.zeppelin_settings set authorization_settings = '"
        + authorizationInfoSaving.toJson() + "' where id = 1");
  }

  @Override
  public NotebookAuthorizationInfoSaving loadNotebookAuthorization()
      throws IOException
  {
    LOGGER.info("retrive notebook authorization from db");
    String json = readFromDB("select authorization_settings from "
        + "datalake_console.zeppelin_settings where id = 1");
    return NotebookAuthorizationInfoSaving.fromJson(json);
  }

  @Override
  public String loadCredentials()
      throws IOException
  {
    LOGGER.info("retrive notebook credentials from db");
    return readFromDB("select credentials_settings from "
        + "datalake_console.zeppelin_settings where id = 1");
  }

  @Override
  public void saveCredentials(String credentials)
      throws IOException
  {
    LOGGER.info("save credentials from db");
    writetoDB("update datalake.console.zepplin_settings set credentials_settings ='"
        + credentials + "' where id = 1");
  }

  public synchronized String readFromDB(String infoQuery)
  {
    try {
      connection = dbConfigStorage.getConnection();
      ResultSet rs = connection.prepareStatement(infoQuery).executeQuery();
      rs.first();
      return rs.getString(1);
    }
    catch (SQLException e) {
      LOGGER.error("could not retrive config info from db" + e);
    }
    finally {
      try {
        connection.close();
      }
      catch (SQLException e) {
        LOGGER.error("could not retrive config info from db" + e);
      }
    }
    return "";
  }

  public synchronized void writetoDB(String infoQuery)
  {
    try {
      connection = dbConfigStorage.getConnection();
      connection.prepareStatement(infoQuery).executeUpdate();
    }
    catch (SQLException e) {
      LOGGER.error("could not retrive config info from db" + e);
    }
    finally {
      try {
        connection.close();
      }
      catch (SQLException e) {
        LOGGER.error("could not retrive config info from db" + e);
      }
    }
  }
}
