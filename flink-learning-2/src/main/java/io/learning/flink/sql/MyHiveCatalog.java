package io.learning.flink.sql;

import javax.annotation.Nullable;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

public class MyHiveCatalog extends HiveCatalog {

  public MyHiveCatalog(String catalogName, @Nullable String defaultDatabase,
      @Nullable String hiveConfDir) {
    super(catalogName, defaultDatabase, hiveConfDir);
  }

  public MyHiveCatalog(String catalogName, @Nullable String defaultDatabase,
      @Nullable String hiveConfDir, String hiveVersion) {
    super(catalogName, defaultDatabase, hiveConfDir, hiveVersion);
  }

  protected MyHiveCatalog(String catalogName, String defaultDatabase,
      @Nullable HiveConf hiveConf, String hiveVersion,
      boolean allowEmbedded) {
    super(catalogName, defaultDatabase, hiveConf, hiveVersion, allowEmbedded);
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    SessionState.setCurrentSessionState(new SessionState(super.getHiveConf()));
    super.createTable(tablePath, table, ignoreIfExists);
  }
}
