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
import com.dataArtisans.localJoins.utils.LocalSourceUtils.BinaryColocatedFileSources;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.Random;

public class LocalJoinDemo {

	private static String basePath = "";

	private static int numFiles = 16;
	private static String pkDirPath = basePath+"/pkDir";
	private static String fkDirPath = basePath+"/fkDir";

	private static String colocatedResult = basePath+"/colocatedJoinResult";
	private static String localReplicationResult = basePath+"/localReplicationJoinResult";
	private static String repartitionResult = basePath+"/repartitionJoinResult";


	public static void main(String[] args) throws Exception {

		if(basePath == null) {
			throw new Exception("Please set basePath!");
		}

		generateTestData();
		joinColocated();
		joinLocalReplication();
		joinRepartition();
	}


	public static void generateTestData() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> pks = env.generateSequence(0L, 10000L);
		DataSet<Long> fks = pks.flatMap(new RandomFKMapper());

		DataSet<Tuple2<Long, String>> pkTuples = pks.map(new RandomTupleMapper());
		DataSet<Tuple2<Long, String>> fkTuples = fks.map(new RandomTupleMapper());

		pkTuples.partitionByHash(0).setParallelism(numFiles).writeAsCsv(pkDirPath).sortLocalOutput(0, Order.ASCENDING);
		fkTuples.partitionByHash(0).setParallelism(numFiles).writeAsCsv(fkDirPath).sortLocalOutput(0, Order.ASCENDING);

		env.execute();

	}


	public static void joinColocated() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		CsvInputFormat<Tuple2<Long, String>> format1 =
				new CsvInputFormat<Tuple2<Long, String>>(new Path("/will/not/be/used"), Long.class, String.class);
		CsvInputFormat<Tuple2<Long, String>> format2 =
				new CsvInputFormat<Tuple2<Long, String>>(new Path("/will/not/be/used"), Long.class, String.class);

		TupleTypeInfo<Tuple2<Long, String>> type1 = new TupleTypeInfo<Tuple2<Long, String>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);
		TupleTypeInfo<Tuple2<Long, String>> type2 = new TupleTypeInfo<Tuple2<Long, String>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		BinaryColocatedFileSources<Tuple2<Long, String>, Tuple2<Long, String>> colocator =
				new BinaryColocatedFileSources<Tuple2<Long, String>, Tuple2<Long, String>>(
						env, format1, format2, type1, type2, "f0", "f0");

		for(int i=1; i<=numFiles; i++) {
			colocator.addColocatedFiles(pkDirPath+"/"+i, fkDirPath+"/"+i, "localhost");
		}

		DataSet<Tuple2<Long, String>> input1 = colocator.getFirstColocatedFileDataSource(env, numFiles);
		DataSet<Tuple2<Long, String>> input2 = colocator.getSecondColocatedFileDataSource(env, numFiles);

		input1.join(input2).where(0).equalTo(0)
				.projectFirst(0,1).projectSecond(1).setParallelism(numFiles)
				.writeAsCsv(colocatedResult).setParallelism(1)
					.sortLocalOutput(0, Order.ASCENDING)
					.sortLocalOutput(1, Order.ASCENDING)
					.sortLocalOutput(2, Order.ASCENDING);

		System.out.println(env.getExecutionPlan());

		env.execute();
	}

	public static void joinLocalReplication() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		TupleTypeInfo<Tuple2<Long, String>> type1 = new TupleTypeInfo<Tuple2<Long, String>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);
		TupleTypeInfo<Tuple2<Long, String>> type2 = new TupleTypeInfo<Tuple2<Long, String>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		CsvInputFormat<Tuple2<Long, String>> format1 =
				new CsvInputFormat<Tuple2<Long, String>>(new Path(pkDirPath), Long.class, String.class);
		CsvInputFormat<Tuple2<Long, String>> format2 =
				new CsvInputFormat<Tuple2<Long, String>>(new Path("/will/not/be/used"), Long.class, String.class);

		LocalSourceUtils.StrictlyLocalFileDataSource strictlyLocal =
				new LocalSourceUtils.StrictlyLocalFileDataSource<Tuple2<Long, String>>(env, numFiles, format2, type2);

		for(int i=1; i<=numFiles; i++) {
			strictlyLocal.addLocalFile(fkDirPath+"/"+i, "localhost");
		}

		DataSet<Tuple2<Long, String>> input1 = LocalSourceUtils.getReplicatedSource(
				env, numFiles, format1, type1);

		DataSet<Tuple3<String, Integer, Double>> input2 = strictlyLocal.getStrictlyLocalFileDataSource();

		input1.join(input2).where(0).equalTo(0)
				.projectFirst(0,1).projectSecond(1).setParallelism(numFiles)
				.writeAsCsv(localReplicationResult).setParallelism(1)
				.sortLocalOutput(0, Order.ASCENDING)
				.sortLocalOutput(1, Order.ASCENDING)
				.sortLocalOutput(2, Order.ASCENDING);

		System.out.println(env.getExecutionPlan());

		env.execute();

	}

	public static void joinRepartition() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(numFiles);

		CsvInputFormat<Tuple2<Long, String>> format1 =
				new CsvInputFormat<Tuple2<Long, String>>(new Path("/will/not/be/used"), Long.class, String.class);
		CsvInputFormat<Tuple2<Long, String>> format2 =
				new CsvInputFormat<Tuple2<Long, String>>(new Path("/will/not/be/used"), Long.class, String.class);

		TupleTypeInfo<Tuple2<Long, String>> type1 = new TupleTypeInfo<Tuple2<Long, String>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);
		TupleTypeInfo<Tuple2<Long, String>> type2 = new TupleTypeInfo<Tuple2<Long, String>>(
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		BinaryColocatedFileSources<Tuple2<Long, String>, Tuple2<Long, String>> colocator =
				new BinaryColocatedFileSources<Tuple2<Long, String>, Tuple2<Long, String>>(
						env, format1, format2, type1, type2, "f0", "f0");

		for(int i=1; i<=numFiles; i++) {
			colocator.addColocatedFiles(pkDirPath+"/"+i, fkDirPath+"/"+i, "localhost");
		}

		DataSet<Tuple2<Long, String>> input1 = env.readCsvFile(pkDirPath).types(Long.class, String.class);
		DataSet<Tuple2<Long, String>> input2 = env.readCsvFile(fkDirPath).types(Long.class, String.class);

		input1.join(input2, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST).where(0).equalTo(0)
				.projectFirst(0,1).projectSecond(1)
				.writeAsCsv(repartitionResult).setParallelism(1)
				.sortLocalOutput(0, Order.ASCENDING)
				.sortLocalOutput(1, Order.ASCENDING)
				.sortLocalOutput(2, Order.ASCENDING);

		System.out.println(env.getExecutionPlan());

		env.execute();
	}



	public static class RandomFKMapper implements FlatMapFunction<Long, Long> {

		Random rand = new Random(1234L);

		@Override
		public void flatMap(Long val, Collector<Long> out) throws Exception {
			int repeat = 1 + rand.nextInt(10);
			for(int i=0; i<repeat; i++) {
				out.collect(val);
			}
		}
	}

	public static class RandomTupleMapper implements MapFunction<Long, Tuple2<Long, String>> {

		Random rand = new Random(1234L);

		@Override
		public Tuple2<Long, String> map(Long val) throws Exception {
			String strVal = "Str_"+rand.nextLong();
			return new Tuple2<Long, String>(val, strVal);
		}
	}
}
