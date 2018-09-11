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

package org.apache.flink.api.common.io;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;

/**
 * The base interface for outputs that consumes records. The output format
 * describes how to store the final records, for example in a file.
 * <p>
 * The life cycle of an output format is the following:
 * <ol>
 *   <li>configure() is invoked a single time. The method can be used to implement initialization from
 *       the parameters (configuration) that may be attached upon instantiation.</li>
 *   <li>Each parallel output task creates an instance, configures it and opens it.</li>
 *   <li>All records of its parallel instance are handed to the output format.</li>
 *   <li>The output format is closed</li>
 * </ol>
 * 
 * @param <IT> The type of the consumed records. 
 */
@Public
public interface OutputFormat<IT> extends Serializable {
	
	/**
	 * Configures this output format. Since output formats are instantiated generically and hence parameterless, 
	 * this method is the place where the output formats set their basic fields based on configuration values.
	 * <p>
	 * This method is always called first on a newly instantiated output format. 
	 *  
	 * @param parameters The configuration with all parameters.
	 */
	void configure(Configuration parameters);
	
	/**
	 * parallel n. 平行线；对比 vt. 使…与…平行 adj. 平行的；类似的，相同的
	 * guaranteed adj. 有保证的，；有人担保的 v. 担保（guarantee的过去式和过去分词）
	 * Opens a parallel instance of the output format to store the result of its parallel instance.
	 * <p>
	 * When this method is called, the output format it guaranteed to be configured.
	 * 
	 * @param taskNumber The number of the parallel instance.
	 * @param numTasks The number of parallel tasks.
	 * @throws IOException Thrown, if the output could not be opened due to an I/O problem.
	 */
	void open(int taskNumber, int numTasks) throws IOException;
	
	
	/**
	 * Adds a record to the output.
	 * <p>
	 * When this method is called, the output format it guaranteed to be opened.
	 * 
	 * @param record The records to add to the output.
	 * @throws IOException Thrown, if the records could not be added to to an I/O problem.
	 */
	void writeRecord(IT record) throws IOException;
	
	/**
	 * marks n. 标记，记号；唛头，分数；台面标志
	 * channels n. 通道；频道；隧道；水槽；录音声道（channel的复数形式） v. 输送；开槽；形成凹槽（channel的第三人称单数形式）
	 * release vt. 释放；发射；让与；允许发表 n. 释放；发布；让与
	 * Method that marks the end of the life-cycle of parallel output instance. Should be used to close
	 * channels and streams and release resources.
	 * After this method returns without an error, the output is assumed to be correct.
	 * <p>
	 * When this method is called, the output format it guaranteed to be opened.
	 *  
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	void close() throws IOException;
}

