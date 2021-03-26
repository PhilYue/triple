/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

// transfer
const (
	// TRIPLE is triple protocol name
	TRIPLE = "tri"

	// DefaultHttp2ControllerReadBufferSize is default read buffer size of triple client/server
	DefaultHttp2ControllerReadBufferSize = 1000000

	// DefaultTimeout is default timeout seconds of triple client
	DefaultTimeout = 15
)

// serializer
// TripleSerializerName is the type of triple serializer
type TripleSerializerName string

const (
	// PBSerializerName is the default serializer name, triple use pb as serializer.
	PBSerializerName = TripleSerializerName("protobuf")

	// HessianSerializerName is the serializer with pb wrapped with hessian2
	HessianSerializerName = TripleSerializerName("hessian2")
)
