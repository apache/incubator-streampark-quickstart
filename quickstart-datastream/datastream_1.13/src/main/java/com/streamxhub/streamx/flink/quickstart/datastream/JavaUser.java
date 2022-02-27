/*
 * Copyright (c) 2019 The StreamX Project
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.streamxhub.streamx.flink.quickstart.datastream;

import java.io.Serializable;

/**
 * @author benjobs
 */
public class JavaUser implements Serializable {
    public String name;
    public Integer age;
    public Integer gender;
    public String address;

    public String toSql() {
        return String.format(
                "insert into t_user(`name`,`age`,`gender`,`address`) value('%s',%d,%d,'%s')",
                name,
                age,
                gender,
                address);
    }

}
