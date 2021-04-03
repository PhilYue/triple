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

package codec

import (
	hessian "github.com/apache/dubbo-go-hessian2"
	proto2 "github.com/dubbogo/triple/internal/codec/proto"
	"github.com/golang/protobuf/proto"
)

import (
	"github.com/dubbogo/triple/pkg/common"
)

func init() {
	common.SetDubbo3Serializer(common.PBSerializerName, NewProtobufCodeC)
	common.SetDubbo3Serializer(common.HessianSerializerName, NewHessianCodeC)
	common.SetDubbo3Serializer(common.TripleHessianWrapperSerializerName, NewTripleHessianWrapperSerializer)
}

// ProtobufCodeC is the protobuf impl of Dubbo3Serializer interface
type ProtobufCodeC struct {
}

// Marshal serialize interface @v to bytes
func (p *ProtobufCodeC) MarshalRequest(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

// Unmarshal deserialize @data to interface
func (p *ProtobufCodeC) UnmarshalRequest(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

// Marshal serialize interface @v to bytes
func (p *ProtobufCodeC) MarshalResponse(v interface{}) ([]byte, error) {
	return p.MarshalRequest(v)
}

// Unmarshal deserialize @data to interface
func (p *ProtobufCodeC) UnmarshalResponse(data []byte, v interface{}) error {
	return p.UnmarshalRequest(data, v)
}

// NewProtobufCodeC returns new ProtobufCodeC
func NewProtobufCodeC() common.Dubbo3Serializer {
	return &ProtobufCodeC{}
}

//// HessianTransferPackage is hessian encode package, wrapping pb data
//type HessianTransferPackage struct {
//	Length int
//	Type   string
//	Data   []byte
//}
//
//func (HessianTransferPackage) JavaClassName() string {
//	return "org.apache.dubbo.HessianPkg"
//}

type HessianCodeC struct {
}

func (h *HessianCodeC) MarshalRequest(v interface{}) ([]byte, error) {
	encoder := hessian.NewEncoder()
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return encoder.Buffer(), nil
}

type HessianUnmarshalStruct struct {
	Val interface{}
}

func (h *HessianCodeC) UnmarshalRequest(data []byte, v interface{}) error {
	decoder := hessian.NewDecoder(data)
	val, err := decoder.Decode()
	v.(*HessianUnmarshalStruct).Val = val
	return err
}

func (h *HessianCodeC) MarshalResponse(v interface{}) ([]byte, error) {
	return h.MarshalRequest(v)
}

func (h *HessianCodeC) UnmarshalResponse(data []byte, v interface{}) error {
	return h.UnmarshalRequest(data, v)
}

// NewHessianCodeC returns new HessianCodeC
func NewHessianCodeC() common.Dubbo3Serializer {
	return &HessianCodeC{}
}

// TripleHessianWrapperSerializer
type TripleHessianWrapperSerializer struct {
	pbSerializer      common.Dubbo3Serializer
	hessianSerializer common.Dubbo3Serializer
}

func NewTripleHessianWrapperSerializer() common.Dubbo3Serializer {
	return &TripleHessianWrapperSerializer{
		pbSerializer:      NewProtobufCodeC(),
		hessianSerializer: NewHessianCodeC(),
	}
}

func (h *TripleHessianWrapperSerializer) MarshalRequest(v interface{}) ([]byte, error) {
	args := v.([]interface{})
	argsBytes := make([][]byte, 0)
	argsTypes := make([]string, 0)
	for _, v := range args {
		data, err := h.hessianSerializer.MarshalRequest(v)
		if err != nil {
			return nil, err
		}
		argsBytes = append(argsBytes, data)
		argsTypes = append(argsTypes, getArgType(v))
	}

	wrapperRequest := &proto2.TripleRequestWrapper{
		SerializeType: string(common.HessianSerializerName),
		Args:          argsBytes,
		// todo to java name
		ArgTypes: argsTypes,
	}
	return h.pbSerializer.MarshalRequest(wrapperRequest)
}

func (h *TripleHessianWrapperSerializer) UnmarshalRequest(data []byte, v interface{}) error {
	wrapperRequest := proto2.TripleRequestWrapper{}
	err := h.pbSerializer.UnmarshalRequest(data, &wrapperRequest)
	if err != nil {
		return err
	}
	args := []interface{}{}
	for _, v := range wrapperRequest.Args {
		arg := HessianUnmarshalStruct{}
		if err := h.hessianSerializer.UnmarshalRequest(v, &arg); err != nil {
			return err
		}
		args = append(args, arg.Val)
	}
	v.(*HessianUnmarshalStruct).Val = args
	return nil
}

func (h *TripleHessianWrapperSerializer) MarshalResponse(v interface{}) ([]byte, error) {
	data, err := h.hessianSerializer.MarshalResponse(v)
	if err != nil {
		return nil, err
	}
	wrapperRequest := &proto2.TripleResponseWrapper{
		SerializeType: string(common.HessianSerializerName),
		Data:          data,
		Type:          getArgType(v),
	}
	return h.pbSerializer.MarshalResponse(wrapperRequest)
}

func (h *TripleHessianWrapperSerializer) UnmarshalResponse(data []byte, v interface{}) error {
	wrapperResponse := proto2.TripleResponseWrapper{}
	err := h.pbSerializer.UnmarshalResponse(data, &wrapperResponse)
	if err != nil {
		return err
	}
	return h.hessianSerializer.UnmarshalResponse(wrapperResponse.Data, v)
}
