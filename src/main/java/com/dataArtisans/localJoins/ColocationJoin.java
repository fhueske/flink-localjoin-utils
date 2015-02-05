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

import com.dataArtisans.localJoins.utils.LocalSourceUtils.BinaryColocatedFileSources;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;

public class ColocationJoin {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		CsvInputFormat<Tuple2<String, Integer>> format1 =
				new CsvInputFormat<Tuple2<String, Integer>>(new Path("/will/not/be/used"), String.class, Integer.class);
		CsvInputFormat<Tuple2<String, Double>> format2 =
				new CsvInputFormat<Tuple2<String, Double>>(new Path("/will/not/be/used/either"), String.class, Double.class);
		TupleTypeInfo<Tuple2<String, Integer>> type1 = new TupleTypeInfo<Tuple2<String, Integer>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		TupleTypeInfo<Tuple2<String, Double>> type2= new TupleTypeInfo<Tuple2<String, Double>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO);

		BinaryColocatedFileSources<Tuple2<String, Integer>, Tuple2<String, Double>> colocator =
				new BinaryColocatedFileSources<Tuple2<String, Integer>, Tuple2<String, Double>>(env, format1, format2, type1, type2, "0", "0");

		colocator.addColocatedFiles("file:///local/on/host1/input1/partition1", "file:///local/on/host1/input2/partition1", "host1");
		colocator.addColocatedFiles("file:///local/on/host2/input1/partition2", "file:///local/on/host2/input2/partition2", "host2");
		colocator.addColocatedFiles("file:///local/on/host2/input1/partition3", "file:///local/on/host2/input2/partition3", "host2");
		colocator.addColocatedFiles("file:///local/on/host3/input1/partition4", "file:///local/on/host3/input2/partition4", "host3");
		colocator.addColocatedFiles("file:///local/on/host4/input1/partition5", "file:///local/on/host4/input2/partition5", "host4");
		colocator.addColocatedFiles("file:///local/on/host4/input1/partition6", "file:///local/on/host4/input2/partition6", "host4");
		colocator.addColocatedFiles("file:///local/on/host4/input1/partition7", "file:///local/on/host4/input2/partition7", "host4");

		DataSet<Tuple2<String, Integer>> input1 = colocator.getFirstColocatedFileDataSource(env);
		DataSet<Tuple2<String, Double>> input2 = colocator.getSecondColocatedFileDataSource(env);

		input1.join(input2).where(0).equalTo(0)
				.projectFirst(1).projectSecond(2)
				.writeAsCsv("file:///some/local/output/path").sortLocalOutput(1, Order.DESCENDING);

		env.execute();
	}
}
