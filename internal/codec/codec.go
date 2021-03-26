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
	"github.com/golang/protobuf/proto"
	perrors "github.com/pkg/errors"
)

import (
	"github.com/dubbogo/triple/pkg/common"
)

func init() {
	common.SetDubbo3Serializer(common.PBSerializerName, NewProtobufCodeC)
	common.SetDubbo3Serializer(common.HessianSerializerName, NewHessianCodeC)
}

// ProtobufCodeC is the protobuf impl of Dubbo3Serializer interface
type ProtobufCodeC struct {
}

// Marshal serialize interface @v to bytes
func (p *ProtobufCodeC) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

// Unmarshal deserialize @data to interface
func (p *ProtobufCodeC) Unmarshal(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}

// NewProtobufCodeC returns new ProtobufCodeC
func NewProtobufCodeC() common.Dubbo3Serializer {
	return &ProtobufCodeC{}
}

// HessianTransferPackage is hessian encode package, wrapping pb data
type HessianTransferPackage struct {
	Length int
	Type   string
	Data   []byte
}

func (HessianTransferPackage) JavaClassName() string {
	return "org.apache.dubbo.HessianPkg"
}

type HessianCodeC struct {
	protoBufSerilizer common.Dubbo3Serializer
}

func (h *HessianCodeC) Marshal(v interface{}) ([]byte, error) {
	pbData, err := h.protoBufSerilizer.Marshal(v)
	if err != nil {
		return nil, err
	}
	hessianTranseferPkg := HessianTransferPackage{
		Length: len(pbData),
		Data:   pbData,
	}
	encoder := hessian.NewEncoder()
	if err := encoder.Encode(hessianTranseferPkg); err != nil {
		return nil, err
	}
	return encoder.Buffer(), nil
}

func (h *HessianCodeC) Unmarshal(data []byte, v interface{}) error {
	var err error
	var pbStruct interface{}
	decoder := hessian.NewDecoder(data)
	pbStruct, err = decoder.Decode()
	if err != nil {
		return err
	}
	hessianTransferPkg, ok := pbStruct.(*HessianTransferPackage)
	if !ok {
		return perrors.Errorf("hessian serializer gets pkg that not hessianTransfer pkg")
	}
	return h.protoBufSerilizer.Unmarshal(hessianTransferPkg.Data, v)
}

// NewHessianCodeC returns new HessianCodeC
func NewHessianCodeC() common.Dubbo3Serializer {
	hessian.RegisterPOJO(&HessianTransferPackage{})
	return &HessianCodeC{
		protoBufSerilizer: NewProtobufCodeC(),
	}
}
