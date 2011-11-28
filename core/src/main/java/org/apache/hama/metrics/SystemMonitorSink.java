/**
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
package org.apache.hama.metrics;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provide the latest metrics information to master server. 
 */
public final class SystemMonitorSink implements MetricsSink {

  public static final Log LOG = LogFactory.getLog(SystemMonitorSink.class);

  private final AtomicReference<MetricsRecord> holder =
    new AtomicReference<MetricsRecord>();

  @Override
  public void putMetrics(final MetricsRecord record){
    if(LOG.isDebugEnabled()){
      LOG.debug("MetricsRecord size "+
        (null!=record?record.metrics().size():"0"));
    }
    holder.set(record);
  }

  /**
   * Obtain the latest metrics record.
   * @return the latest metrics record.
   */
  public MetricsRecord get(){
    return holder.get();
  }

  @Override
  public void flush(){}


}
