package mygrp.mydataflow;

import java.beans.Transient;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.Options;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

class JdbcDriverInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	@Transient
	public Connection getConnection() throws SQLException {
		var props = new Properties();
		props.setProperty("user", this.getUser());
		props.setProperty("password", this.getPassword());
		props.setProperty("ssl", "false");

		return DriverManager.getConnection(this.getUrl(), props);
	}
	private String driver;

	public String getDriver() {
		return driver;
	}

	private String url;

	public String getUrl() {
		return url;
	}

	private String user;

	public String getUser() {
		return user;
	}

	private String password;

	public String getPassword() {
		return password;
	}

	JdbcDriverInfo(String driver, String url, String user, String password) {
		this.driver = driver;
		this.url = url;
		this.user = user;
		this.password = password;
	}
}

public class RecordsetSchema implements Serializable{

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RecordsetSchema.class);
	Schema sbf = null;
	public Row createRow(ResultSet rs) throws SQLException {
		var r = Row.withSchema(this.sbf);
		for (var i = 1; i <= this.sbf.getFieldCount(); i++) {
			r.addValue(rs.getObject(i));
		}
		var row = r.build();
		LOG.info(row.toString());
		return row;
	}
	public Schema getSchema() {
		return this.sbf;
	}
	public static class DBTable implements Serializable{
		public static class Builder extends DBTable {
			protected ArrayList<DBColumn.Builder> columns;
			public Builder() {
			}
			public void setColumnCount(int columnCount) {
				this.columnCount = columnCount;
			}
			public void setColumns(ArrayList<DBColumn.Builder> columns) {
				this.columns = columns;
			}

		}
		protected DBTable() {
		}

		public DBTable(ResultSetMetaData md) throws SQLException {
			this.columnCount = md.getColumnCount();
			this.columns = new ArrayList<DBColumn>(this.columnCount);
			for (var i = 1; i <= this.columnCount; i++) {
				var column = new DBColumn.Builder();
				column.catalogName = md.getCatalogName(i);
				column.tableName = md.getTableName(i);
				column.name = md.getColumnName(i);
				column.type = md.getColumnType(i);
				column.typeName = md.getColumnTypeName(i);
				column.nullable = md.isNullable(i) == java.sql.ResultSetMetaData.columnNullable;
				this.columns.add(column);
			}
		}

		protected int columnCount;
		public int getColumnCount() {
			return columnCount;
		}
		public ArrayList<DBColumn> getColumns() {
			return (ArrayList<DBColumn>) columns;
		}
		protected ArrayList<DBColumn> columns;
		public static class DBColumn implements Serializable{
			protected String catalogName;
			public String getCatalogName() {
				return catalogName;
			}

			protected String tableName;
			public String getTableName() {
				return tableName;
			}

			protected String name;
			public String getName() {
				return name;
			}
			protected String typeName;
			public String getTypeName() {
				return typeName;
			}
			protected int type;
			public int getType() {
				return type;
			}
			protected boolean nullable;
			public boolean getNullable() {
				return nullable;
			}
			public static class Builder extends DBColumn implements Serializable{
				public void setCatalogName(String catalogName) {
					this.catalogName = catalogName;
				}
				public void setTableName(String tableName) {
					this.tableName = tableName;
				}
				public void setName(String name) {
					this.name = name;
				}
				public void setTypeName(String typeName) {
					this.typeName = typeName;
				}
				public void setType(int type) {
					this.type = type;
				}
				public void setNullable(boolean nullable) {
					this.nullable = nullable;
				}
			}
		}
		public static DBTable load(String filename) {
			FileInputStream fr = null;
			DBTable ret = null;
			try {
				fr = new FileInputStream(filename);			
				var mapper = new ObjectMapper(new YAMLFactory());
				ret = mapper.readValue(fr, DBTable.class);
			} catch (Exception e) {
			}
			return ret;
		}
		public static void save(String filename, DBTable schema) throws Exception {
			if (filename==null) {
				return;
			}
			var fw = new FileOutputStream(filename);
			var mapper = new ObjectMapper(new YAMLFactory());
			mapper.writeValue(fw, schema);
		}
	}
	public RecordsetSchema(DBTable md) throws Exception {

		var sb = Schema.builder();
		for (var i = 0; i < md.getColumnCount(); i++) {
			var column = md.getColumns().get(i);
			var ti = column.getType();
			var name = column.getName();
			LOG.info(String.format("%d:%s", ti, column.getTypeName()));
			Schema.Field f = null;
			switch (ti) {
			case java.sql.Types.VARCHAR:
				LOG.info("VARCHAR");
				f = Schema.Field.of(name, FieldType.STRING);
				break;
			case java.sql.Types.DATE:
			case java.sql.Types.TIME:
				LOG.info("DATE");
				f = Schema.Field.of(name, FieldType.DATETIME);
				break;
			case java.sql.Types.DECIMAL:
				LOG.info("DECIMAL");
				f = Schema.Field.of(name, FieldType.DECIMAL);
				break;
			default:
				f = Schema.Field.of(name, FieldType.STRING);
				break;

			}
			f = f.withNullable(column.getNullable()).withOptions(
				Options.builder().setOption("tableName", Schema.FieldType.STRING, column.getTableName())
				.setOption("catalog", Schema.FieldType.STRING, column.getCatalogName())
			);
			sb = sb.addField(f);
		}
		this.sbf = sb.build();

		LOG.info(this.sbf.toString());

	}

}
