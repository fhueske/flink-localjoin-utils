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

package com.dataArtisans.localJoins.utils;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.ReplicatingInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileInputSplit;

public class LocalSourceUtils {

	public static <T> DataSource<T> getReplicatedSource(ExecutionEnvironment env, FileInputFormat<T> replicatedFormat, TypeInformation<T> type) {
		return getReplicatedSource(env, env.getDegreeOfParallelism(), replicatedFormat, type);
	}

	public static <T> DataSource<T> getReplicatedSource(ExecutionEnvironment env, int dop, FileInputFormat<T> replicatedFormat, TypeInformation<T> type) {
		return env.createInput(new ReplicatingInputFormat<T, FileInputSplit>(replicatedFormat), type).setParallelism(dop);
	}

	public static class StrictlyLocalFileDataSource<T> {

		ExecutionEnvironment env;
		int dop;
		StrictlyLocalFileInputFormat<T> format;
		TypeInformation<T> type;

		public StrictlyLocalFileDataSource(ExecutionEnvironment env, FileInputFormat<T> format, TypeInformation<T> type) {
			this(env, env.getDegreeOfParallelism(), format, type);
		}

		public StrictlyLocalFileDataSource(ExecutionEnvironment env, int dop, FileInputFormat<T> format, TypeInformation<T> type) {
			this.env = env;
			this.dop = dop;
			this.format = new StrictlyLocalFileInputFormat<T>(format);
			this.type = type;
		}

		public void addLocalFile(String path, String host) {
			this.format.addLocalFile(path, host);
		}

		public DataSource<T> getStrictlyLocalFileDataSource() {
			return env.createInput(format, type).setParallelism(dop);
		}

	}

	public static class BinaryColocatedFileSources<T1, T2> {

		ExecutionEnvironment env;
		int dop;
		StrictlyLocalFileInputFormat<T1> format1;
		StrictlyLocalFileInputFormat<T2> format2;
		TypeInformation<T1> type1;
		TypeInformation<T2> type2;
		String partitionKeys1;
		String partitionKeys2;

		public BinaryColocatedFileSources(ExecutionEnvironment env,
										  FileInputFormat<T1> format1,
										  FileInputFormat<T2> format2,
										  TypeInformation<T1> type1,
										  TypeInformation<T2> type2,
										  String partitionKey1,
										  String partitionKey2) {

			this(env, env.getDegreeOfParallelism(),
					format1, format2, type1, type2, partitionKey1, partitionKey2);
		}

		public BinaryColocatedFileSources(ExecutionEnvironment env, int dop,
									FileInputFormat<T1> format1,
									FileInputFormat<T2> format2,
									TypeInformation<T1> type1,
									TypeInformation<T2> type2,
									String partitionKey1,
									String partitionKey2) {

			this.env = env;
			this.dop = dop;
			this.format1 = new StrictlyLocalFileInputFormat(format1);
			this.format2 = new StrictlyLocalFileInputFormat(format2);
			this.type1 = type1;
			this.type2 = type2;
			this.partitionKeys1 = partitionKey1;
			this.partitionKeys2 = partitionKey2;
		}

		public void addColocatedFiles(String path1, String path2, String host) {
			this.format1.addLocalFile(path1, host);
			this.format2.addLocalFile(path2, host);
		}

		public DataSource<T1> getFirstColocatedFileDataSource(ExecutionEnvironment env) {
			return getFirstColocatedFileDataSource(env, env.getDegreeOfParallelism());
		}

		public DataSource<T1> getFirstColocatedFileDataSource(ExecutionEnvironment env, int dop) {
			DataSource<T1> firstSource = env.createInput(format1, type1).setParallelism(dop);
			firstSource.getSplitDataProperties().splitsPartitionedBy("partitioning_"+this.hashCode(), partitionKeys1);
			return firstSource;
		}

		public DataSource<T2> getSecondColocatedFileDataSource(ExecutionEnvironment env) {
			return getSecondColocatedFileDataSource(env, env.getDegreeOfParallelism());
		}

		public DataSource<T2> getSecondColocatedFileDataSource(ExecutionEnvironment env, int dop) {
			DataSource<T2> secondSource = env.createInput(format2, type2).setParallelism(dop);
			secondSource.getSplitDataProperties().splitsPartitionedBy("partitioning_"+this.hashCode(), partitionKeys2);
			return secondSource;
		}

	}

}
