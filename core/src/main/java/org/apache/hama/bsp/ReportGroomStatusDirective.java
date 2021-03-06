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
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

/**
 * Reports status of GroomServer.
 */
public class ReportGroomStatusDirective extends Directive implements Writable {
  public static final Log LOG = LogFactory.getLog(ReportGroomStatusDirective.class);

  private GroomServerStatus status;

  public ReportGroomStatusDirective(){ super(); }
  
  public ReportGroomStatusDirective(GroomServerStatus status) {
    super(Directive.Type.Response);
    this.status = status;
  }

  public GroomServerStatus getStatus() {
    return this.status;
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.status.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.status = new GroomServerStatus();
    this.status.readFields(in);
  }
}
