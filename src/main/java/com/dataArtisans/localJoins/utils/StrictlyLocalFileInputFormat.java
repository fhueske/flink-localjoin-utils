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
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.StrictlyLocalAssignment;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.IOException;
import java.util.ArrayList;

public class StrictlyLocalFileInputFormat<T> extends FileInputFormat<T> implements StrictlyLocalAssignment {

	FileInputFormat<T> wrappedFormat;

	ArrayList<LocatableInputSplit> splits;

	public StrictlyLocalFileInputFormat(FileInputFormat<T> wrappedFormat) {
		this.wrappedFormat = wrappedFormat;
		this.splits = new ArrayList<LocatableInputSplit>();
	}

	public void addLocalFile(String filePath, String hostName) {

		FileInputSplit split = new FileInputSplit(
				this.splits.size(), new Path(filePath), 0, FileInputFormat.READ_WHOLE_SPLIT_FLAG, new String[]{hostName} );

		this.splits.add(split);
	}

	@Override
	public FileBaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
		return new FileBaseStatistics(-1L, -1L, -1L);
	}

	@Override
	public FileInputSplit[] createInputSplits(int i) throws IOException {
		return splits.toArray(new FileInputSplit[splits.size()]);
	}

	@Override
	public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] fileInputSplits) {
		// won't be used
		return null;
	}

	@Override
	public void configure(Configuration configuration) {
		wrappedFormat.configure(configuration);
	}

	@Override
	public void open(FileInputSplit fileInputSplit) throws IOException {
		wrappedFormat.open(fileInputSplit);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return wrappedFormat.reachedEnd();
	}

	@Override
	public T nextRecord(T t) throws IOException {
		return wrappedFormat.nextRecord(t);
	}

	@Override
	public void close() throws IOException {
		wrappedFormat.close();
	}
}
