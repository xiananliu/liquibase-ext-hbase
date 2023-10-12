package liquibase.ext.hbase.sqlgenerator.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.CreateTableStatement;

public class HbaseCreateDatabaseChangeLogLockTableGenerator extends 
  CreateDatabaseChangeLogLockTableGenerator {

  @Override
  public int getPriority() {
      return PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, 
      Database database, SqlGeneratorChain sqlGeneratorChain) {
  
    CreateTableStatement createTableStatement = new CreateTableStatement(database.getLiquibaseCatalogName(), 
        database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName())
        .setTablespace(database.getLiquibaseTablespaceName())
        .addPrimaryKeyColumn("ID", DataTypeFactory.getInstance().fromDescription("BIGINT", database), 
            null, null, null, new NotNullConstraint())
        .addColumn("LOCKED", DataTypeFactory.getInstance().fromDescription("BOOLEAN", database), null, null)
        .addColumn("LOCKGRANTED", DataTypeFactory.getInstance().fromDescription("DATE", database))
        .addColumn("LOCKEDBY", DataTypeFactory.getInstance().fromDescription("VARCHAR(255)", database));
    List<Sql> sql = new ArrayList<Sql>();
    sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database)));
    return sql.toArray(new Sql[sql.size()]);
  }
}