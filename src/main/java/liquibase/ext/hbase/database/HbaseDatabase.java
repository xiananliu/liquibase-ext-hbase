/**
 * Copyright 2010 Open Pricer
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.hbase.database;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SequenceCurrentValueFunction;
import liquibase.statement.SequenceNextValueFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Sequence;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;

/**
 * HBase implementation for liquibase
 *
 */
public class HbaseDatabase extends AbstractJdbcDatabase {
    private final String databaseProductName;
    private final String prefix;
    private final String databaseDriver;
    private final int PORT_NO = 2181;

    public HbaseDatabase() {
        this("Hbase", "jdbc:phoenix", "org.apache.phoenix.jdbc.PhoenixDriver");
    }

    public HbaseDatabase(String databaseProductName, String prefix,
                         String databaseDriver) {
        this.databaseProductName = databaseProductName;
        this.prefix = prefix;
        this.databaseDriver = databaseDriver;
        super.sequenceNextValueFunction = "NEXT VALUE FOR %S";
        super.sequenceCurrentValueFunction = "CURRENT VALUE FOR  %s";

        this.addReservedWords(Arrays.asList("SELECT", "FROM", "WHERE", "NOT", "AND", "OR",
                "NULL", "TRUE", "FALSE", "LIKE", "ILIKE", "AS", "OUTER", "ON", "OFF", "IN",
                "GROUP", "HAVING", "ORDER", "BY", "ASC", "DESC", "NULLS", "LIMIT", "FIRST",
                "LAST", "CASE", "WHEN", "THEN", "ELSE", "END", "EXISTS", "IS", "FIRST",
                "DISTINCT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "BETWEEN", "UPSERT",
                "INTO", "VALUES", "DELETE", "CREATE", "DROP", "PRIMARY", "KEY", "ALTER",
                "COLUMN", "SESSION", "TABLE", "SCHEMA", "ADD", "SPLIT", "EXPLAIN", "VIEW",
                "IF", "CONSTRAINT", "TABLES", "ALL", "INDEX", "INCLUDE", "WITHIN", "SET",
                "CAST", "ACTIVE", "USABLE", "UNUSABLE", "DISABLE", "REBUILD", "ARRAY",
                "SEQUENCE", "START", "WITH", "INCREMENT", "NEXT", "CURRENT", "VALUE",
                "FOR", "CACHE", "LOCAL", "ANY", "SOME", "MINVALUE", "MAXVALUE", "CYCLE",
                "CASCADE", "UPDATE", "STATISTICS", "COLUMNS", "TRACE", "ASYNC", "SAMPLING",
                "TABLESAMPLE", "UNION", "FUNCTION", "AS", "TEMPORARY", "RETURNS", "USING",
                "JAR", "DEFAULTVALUE", "CONSTANT", "REPLACE", "LIST", "JARS", "ROW_TIMESTAMP",
                "USE", "OFFSET", "FETCH", "DECLARE", "CURSOR", "OPEN", "CLOSE", "ROW", "ROWS",
                "ONLY", "EXECUTE", "UPGRADE", "DEFAULT", "DUPLICATE", "IGNORE", "IMMUTABLE"));
    }

    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn)
            throws DatabaseException {
        return databaseProductName.equals("Hbase");
    }


    public String getDefaultDriver(String url) {
        if (url.startsWith(prefix)) {
            return databaseDriver;
        }
        return null;
    }

    public String getShortName() {
        return databaseProductName.toLowerCase();
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return databaseProductName;
    }

    public Integer getDefaultPort() {
        return PORT_NO;
    }

    @Override
    public String getCurrentDateTimeFunction() {
        return "current_time()";
    }


  /*
  @Override
  public String getDefaultCatalogName() {
    return "";
  }*/

    @Override
    protected String getConnectionSchemaName() {
        boolean flag = false;
        String schemaName = "";
        try {
            String tokens[] = super.getConnection().getURL().split(";");
            if (tokens.length > 1) {
                String schemaInfo[] = tokens[1].split(":");
                if (schemaInfo.length > 1) {
                    String[] schemaDetails = schemaInfo[0].split("=");
                    if (schemaDetails.length > 1) {
                        schemaName = schemaDetails[1].toUpperCase();
                        flag = true;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Using \"\" (default) as schema name. "
                    + "Please use \"schema_name\" param to set specific schema name in driver url. "
                    + "For example, jdbc:phoenix:localhost;schema_name=<SCHEMA_NAME>:2181:/hbase"
                    + "\t[ex: " + e.getMessage() + "]");
        }

        if (!flag) {
            System.out.println("Using \"\" (default) as schema name. "
                    + "Please use \"schema_name\" param to set specific schema name in driver url. "
                    + "For example, jdbc:phoenix:localhost;schema_name=<SCHEMA_NAME>:2181:/hbase"
                    + "\t[url: " + super.getConnection().getURL() + "]");

        }
        return schemaName;
    }

    public boolean supportsInitiallyDeferrableColumns() {
        return true;
    }

    public boolean supportsTablespaces() {
        return false;
    }

    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    @Override
    public CatalogAndSchema getSchemaFromJdbcInfo(String catalogName, String schemaName) {
        return new CatalogAndSchema(null, schemaName);
    }

    @Override
    public String escapeObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        if (quotingStrategy == ObjectQuotingStrategy.QUOTE_ALL_OBJECTS) {
            return super.escapeObjectName(objectName, objectType);
        }
        if (objectName != null &&
                quotingStrategy != ObjectQuotingStrategy.QUOTE_ALL_OBJECTS &&
                isReservedWord(objectName.toUpperCase())) {
            return "\"" + objectName.toUpperCase() + "\"";
        }
        return objectName;
    }

    /**
     * Exact Copy of AbstractJdbcDatabase#isCurrentTimeFunction to
     * satisfy HbaseDatabase#generateDatabaseFunctionValue()
     *
     * @param functionValue
     * @return
     */
    public boolean isCurrentTimeFunction(final String functionValue) {
        if (functionValue == null) {
            return false;
        }

        return functionValue.startsWith("current_timestamp")
                || functionValue.startsWith("current_datetime")
                || functionValue.equalsIgnoreCase(getCurrentDateTimeFunction());
    }

    /**
     * Copy of AbstractJdbcDatabase#generateDatabaseFunctionValue() with minor diff as
     * described below
     *
     * Internal sequenceName has dot(.) at this moment for sure it prefixed with "<schema_name>."
     * To pass through, we are by passing escapeObjectName method which escapes using
     * AbstractJdbcDatabase#quotingStartCharacter and AbstractJdbcDatabase#quotingEndCharacter
     * Hence, Safer to not allow dot(.) in external sequence names coming through changesets as
     * those chars won't be escaped
     */
    @Override
    public String generateDatabaseFunctionValue(final DatabaseFunction databaseFunction) {
        if (databaseFunction.getValue() == null) {
            return null;
        }
        if (isCurrentTimeFunction(databaseFunction.getValue().toLowerCase())) {
            return getCurrentDateTimeFunction();
        } else if (databaseFunction instanceof SequenceNextValueFunction) {
            if (sequenceNextValueFunction == null) {
                throw new RuntimeException(String.format("next value function for a sequence is not configured for " +
                                "database %s",
                        getDefaultDatabaseProductName()));
            }
            String sequenceName = databaseFunction.getValue();
            if (!sequenceNextValueFunction.contains("'") &&
                    !sequenceName.contains(".")) {
                sequenceName = escapeObjectName(sequenceName, Sequence.class);
            }
            return String.format(sequenceNextValueFunction, sequenceName);
        } else if (databaseFunction instanceof SequenceCurrentValueFunction) {
            if (sequenceCurrentValueFunction == null) {
                throw new RuntimeException(String.format("current value function for a sequence is not configured for" +
                                " database %s",
                        getDefaultDatabaseProductName()));
            }

            String sequenceName = databaseFunction.getValue();
            if (!sequenceCurrentValueFunction.contains("'") &&
                    !sequenceName.contains(".")) {
                sequenceName = escapeObjectName(sequenceName, Sequence.class);
            }
            String s = String.format(sequenceCurrentValueFunction, sequenceName);
            return s;
        } else {
            return databaseFunction.getValue();
        }
    }

    /**
     * Use JDBC escape syntax
     */
    @Override
    public String getDateTimeLiteral(Timestamp date) {
        return "'" + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(date) + "'";
    }

    /**
     * Use JDBC escape syntax
     */
    @Override
    public String getDateLiteral(Date date) {
        return "'" + new SimpleDateFormat("yyyy-MM-dd").format(date) + "'";
    }

    /**
     * Use JDBC escape syntax
     */
    @Override
    public String getTimeLiteral(Time date) {
        return "'" + new SimpleDateFormat("hh:mm:ss.SSS").format(date) + "'";
    }

    /**
     * Hbase allows table creation without any prefixing any schema name.
     * In this case, schema has been treated as "". Tables created under this
     * schema can be accessed either by using only tablename or "".tablename.
     * To differentiate this schema and "null" schema in the code, this method
     * o/p constant has been used.
     *
     * Schema.class in Liquibase Core trims empty schema and convert to "null" value,
     * which can be avoided using this constant for real "" or empty schema.
     *
     * @return
     */
    public String getNativeDefaultSchema() {
        return "HBASE_NATIVE_DEFAULT_SCHEMA";
    }
}
