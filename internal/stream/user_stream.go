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

package stream

import (
	"context"
)
import (
	"github.com/apache/dubbo-go/common/logger"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)
import (
	"github.com/dubbogo/triple/internal/message"
	"github.com/dubbogo/triple/pkg/common"
)

// baseUserStream is the base userstream impl
type baseUserStream struct {
	stream     Stream
	serilizer  common.Dubbo3Serializer
	pkgHandler common.PackageHandler
}

func (ss *baseUserStream) SetHeader(metadata.MD) error {
	return nil
}
func (ss *baseUserStream) SendHeader(metadata.MD) error {
	return nil
}
func (ss *baseUserStream) SetTrailer(metadata.MD) {

}
func (ss *baseUserStream) Context() context.Context {
	return nil
}
func (ss *baseUserStream) SendMsg(m interface{}) error {
	replyData, err := ss.serilizer.MarshalRequest(m)
	if err != nil {
		logger.Error("sen msg error with msg = ", m)
		return err
	}
	rspFrameData := ss.pkgHandler.Pkg2FrameData(replyData)
	ss.stream.PutSend(rspFrameData, message.DataMsgType)
	return nil
}

func (ss *baseUserStream) RecvMsg(m interface{}) error {
	recvChan := ss.stream.GetRecv()
	readBuf := <-recvChan
	if readBuf.Buffer == nil {
		return errors.Errorf("user stream closed!")
	}
	pkgData, _ := ss.pkgHandler.Frame2PkgData(readBuf.Bytes())
	if err := ss.serilizer.UnmarshalResponse(pkgData, m); err != nil {
		return err
	}
	return nil
}

// serverUserStream can be throw to grpc, and let grpc use it
type serverUserStream struct {
	baseUserStream
}

func newServerUserStream(s Stream, serilizer common.Dubbo3Serializer, pkgHandler common.PackageHandler) *serverUserStream {
	return &serverUserStream{
		baseUserStream: baseUserStream{
			serilizer:  serilizer,
			pkgHandler: pkgHandler,
			stream:     s,
		},
	}
}

// clientUserStream can be throw to grpc, and let grpc use it
type clientUserStream struct {
	baseUserStream
}

func (ss *clientUserStream) Header() (metadata.MD, error) {
	return nil, nil
}
func (ss *clientUserStream) Trailer() metadata.MD {
	return nil
}
func (ss *clientUserStream) CloseSend() error {
	// todo
	return nil
}

func NewClientUserStream(s Stream, serilizer common.Dubbo3Serializer, pkgHandler common.PackageHandler) *clientUserStream {
	return &clientUserStream{
		baseUserStream: baseUserStream{
			serilizer:  serilizer,
			pkgHandler: pkgHandler,
			stream:     s,
		},
	}
}
