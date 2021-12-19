/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mygrp.mydataflow;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */


public class JdbcSync {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcSync.class);

	public interface SyncOptions extends PipelineOptions {

		public String getSaveFile();
		public void setSaveFile(String s);

		@Description("Get time from Src DB or Dst DB. You input 'src' if you get src DB.")
		@Default.String("dst")
		public String getTimeDb();
		public void setTimeDb(String s);

		@Description("Source JDBC Driver. ex)org.postgresql.Driver")
		public String getSrcJdbcDriver();
		public void setSrcJdbcDriver(String s);

		public String getSrcJdbcUrl();
		public void setSrcJdbcUrl(String s);

		public String getSrcJdbcUser();
		public void setSrcJdbcUser(String s);

		public String getSrcJdbcPassword();
		public void setSrcJdbcPassword(String s);

		@Description("Destination JDBC Driver. ex)org.postgresql.Driver")
		public String getDstJdbcDriver();
		public void setDstJdbcDriver(String s);

		public String getDstJdbcUrl();
		public void setDstJdbcUrl(String s);

		public String getDstJdbcUser();
		public void setDstJdbcUser(String s);

		public String getDstJdbcPassword();
		public void setDstJdbcPassword(String s);
	}


	public static void main(String[] args) throws Exception {
		PipelineOptionsFactory.register(SyncOptions.class);
		var options = PipelineOptionsFactory.fromArgs(args).withValidation().create().as(SyncOptions.class);
		Pipeline p = Pipeline.create(options);

		var df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		var srcJdbc = new JdbcDriverInfo(options.getSrcJdbcDriver(), options.getDstJdbcUrl(), options.getSrcJdbcUser(),
				options.getSrcJdbcPassword());
		var dstJdbc = new JdbcDriverInfo(options.getDstJdbcDriver(), options.getDstJdbcUrl(), options.getDstJdbcUser(),
				options.getDstJdbcPassword());
		if (options.getRunner().getSimpleName().equals("DataflowRunner")) {
			var dataflowOptions = options.as(DataflowPipelineOptions.class);
			dataflowOptions.setTemplateLocation(dataflowOptions.getTemplateLocation()+"/"+options.getSaveFile()+".json");
		}

		var timejdbc = "src".equals(options.getTimeDb()) ? srcJdbc: dstJdbc;
		final var filepath = "./tbl/"+options.getSaveFile()+".yaml";
		var dbt = RecordsetSchema.DBTable.load(filepath);
		if (dbt == null) {
			var conn = srcJdbc.getConnection();
			var ps = conn.prepareStatement("select * from srctable");
			dbt = new RecordsetSchema.DBTable(ps.getMetaData());
			ps.close();
			RecordsetSchema.DBTable.save(filepath, dbt);
		}
		final var md = dbt;
		var rss = new RecordsetSchema(md);
		p.apply(JdbcIO.<String>read()
				.withDataSourceConfiguration(DataSourceConfiguration.create(timejdbc.getDriver(), timejdbc.getUrl())
					.withUsername(timejdbc.getUser())
					.withPassword(timejdbc.getPassword())
				)
				.withQuery("select ? dt")
				.withStatementPreparator((PreparedStatement preparedStatement)->{
						// TODO Auto-generated method stub
						preparedStatement.setTimestamp(1, Timestamp.valueOf("2021-01-02 15:04:05"));

				}).withRowMapper((ResultSet resultSet) -> df.format(resultSet.getTimestamp("dt")))
				.withCoder(StringUtf8Coder.of())).apply(ParDo.of(new DoFn<String, Row>() {

					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						LOG.info("start");
						var conn = srcJdbc.getConnection();
						conn.prepareStatement("delete from dsttable").execute();
						var ps = conn.prepareStatement("select * from srctable");
						var rss = new RecordsetSchema(md);
						var rs = ps.executeQuery();
						while (rs.next()) {
							var r = rss.createRow(rs);
							c.output(r);

						}
						rs.close();
						ps.close();
					}
				})).setCoder(RowCoder.of(rss.getSchema()))
				.apply(JdbcIO.<Row>write()
						.withDataSourceConfiguration(
								DataSourceConfiguration.create(dstJdbc.getDriver(), dstJdbc.getUrl())
								.withUsername(dstJdbc.getUser())
								.withPassword(dstJdbc.getPassword())
						)
						.withStatement("insert into dsttable values(?, ?)")
						.withPreparedStatementSetter((Row element, PreparedStatement preparedStatement) -> {
							var i = 1;
							for (var f : rss.getSchema().getFields()) {
								preparedStatement.setString(i, element.getString(f.getName()));
								i++;
							}

						})

				);

		p.run().waitUntilFinish();
	}
}
