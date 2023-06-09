/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_MULTI_CALL_PROTOBUFBYTEBUFFERUTIL_H
#define ISEARCH_MULTI_CALL_PROTOBUFBYTEBUFFERUTIL_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/ProtobufUtil.h"
#include <grpc++/impl/codegen/byte_buffer.h>
#include <grpc++/impl/codegen/proto_utils.h>

namespace multi_call {

class ProtobufByteBufferUtil {
public:
    static void serializeToBuffer(const google::protobuf::Message &src,
                                  grpc::ByteBuffer *dst);
    static bool deserializeFromBuffer(const grpc::ByteBuffer &src,
                                      google::protobuf::Message *dst);
    static void serializeToBuffer(const google::protobuf::Message *header,
                                  const grpc::Slice *msg,
                                  grpc::ByteBuffer *dst);
    static bool deserializeFromBuffer(const grpc::ByteBuffer &src,
                                      google::protobuf::Message *header,
                                      google::protobuf::Message *msg);
    static bool getHeaderLength(GrpcByteBufferSource &stream,
                                size_t &headerLength);

public:
    static constexpr size_t LENGTH_SIZE = 8;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_PROTOBUFBYTEBUFFERUTIL_H
