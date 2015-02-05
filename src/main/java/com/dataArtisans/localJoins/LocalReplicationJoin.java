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

package com.dataArtisans.localJoins;

import com.dataArtisans.localJoins.utils.LocalSourceUtils;
import com.dataArtisans.localJoins.utils.LocalSourceUtils.StrictlyLocalFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;

public class LocalReplicationJoin {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> replicatedText = LocalSourceUtils.getReplicatedSource(
				env, new TextInputFormat(new Path("/will/be/read/everywhere")), BasicTypeInfo.STRING_TYPE_INFO);

		StrictlyLocalFileSource localCsvSource = new StrictlyLocalFileSource<Tuple3<String, Integer, Double>>(
						env,
						new CsvInputFormat<Tuple3<String, Integer, Double>>(new Path("/will/not/be/used"), String.class, Integer.class, Double.class),
						new TupleTypeInfo<Tuple3<String, Integer, Double>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO));

		localCsvSource.addLocalFile("file:///local/on/host1.1", "host1");
		localCsvSource.addLocalFile("file:///local/on/host1.2", "host1");
		localCsvSource.addLocalFile("file:///local/on/host2.1", "host2");
		localCsvSource.addLocalFile("file:///local/on/host3.1", "host3");
		localCsvSource.addLocalFile("file:///local/on/host3.2", "host3");

		DataSet<Tuple3<String, Integer, Double>> localCsv = localCsvSource.getLocalFileDataSource();

		replicatedText
				// to tuple
				.map(new MapFunction<String, Tuple1<String>>() {

					@Override
					public Tuple1<String> map(String s) throws Exception {
						return new Tuple1<String>(s);
					}
				})
				.joinWithHuge(localCsv).where(0).equalTo(0)
					.projectSecond(0,1,2)
				.writeAsCsv("file:///local/output").sortLocalOutput(1, Order.ASCENDING);

		env.execute();
	}
}
