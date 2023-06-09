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
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"

using namespace std;

namespace multi_call {

void ProtobufByteBufferUtil::serializeToBuffer(
    const google::protobuf::Message &src, grpc::ByteBuffer *dst) {
    grpc::Slice s(src.ByteSizeLong());
    src.SerializeWithCachedSizesToArray(const_cast<uint8_t *>(s.begin()));
    grpc::ByteBuffer buffer(&s, 1);
    dst->Swap(&buffer);
}

bool ProtobufByteBufferUtil::deserializeFromBuffer(
    const grpc::ByteBuffer &src, google::protobuf::Message *dst) {
    GrpcByteBufferSource stream;
    if (!stream.Init(src)) {
        return false;
    }
    return dst->ParseFromZeroCopyStream(&stream);
}

void ProtobufByteBufferUtil::serializeToBuffer(
    const google::protobuf::Message *header,
    const grpc::Slice *msg, grpc::ByteBuffer *dst) {
    size_t headerSize = header->ByteSizeLong();
    grpc::Slice sliceArray[3];
    {
        grpc::Slice headerLengthSlice(LENGTH_SIZE);
        *(size_t *)headerLengthSlice.begin() = headerSize;
        sliceArray[0] = headerLengthSlice;
    }
    {
        grpc::Slice headerSlice(headerSize);
        header->SerializeWithCachedSizesToArray(
            const_cast<uint8_t *>(headerSlice.begin()));
        sliceArray[1] = headerSlice;
    }
    if (nullptr != msg) {
        sliceArray[2] = *msg;
        grpc::ByteBuffer buffer((const grpc::Slice *)&sliceArray, 3);
        dst->Swap(&buffer);
    } else {
        grpc::ByteBuffer buffer((const grpc::Slice *)&sliceArray, 2);
        dst->Swap(&buffer);
    }
}

bool ProtobufByteBufferUtil::deserializeFromBuffer(
    const grpc::ByteBuffer &src, google::protobuf::Message *header,
    google::protobuf::Message *msg) {
    GrpcByteBufferSource stream;
    if (!stream.Init(src)) {
        return false;
    }
    size_t headerLength = 0;
    if (!getHeaderLength(stream, headerLength)) {
        return false;
    }
    assert(LENGTH_SIZE == stream.ByteCount());
    stream.setLimit(LENGTH_SIZE + headerLength);
    if (!header->ParseFromZeroCopyStream(&stream)) {
        return false;
    }
    stream.setLimit(stream.size());
    return msg->ParseFromZeroCopyStream(&stream);
}

bool ProtobufByteBufferUtil::getHeaderLength(GrpcByteBufferSource &stream,
                                             size_t &headerLength) {
    GrpcByteBufferSourceByteReader reader(stream);
    uint8_t *ptr = (uint8_t *)&headerLength;
    for (size_t i = 0; i < LENGTH_SIZE; i++) {
        if (!reader.next(ptr + i)) {
            return false;
        }
    }
    return true;
}

} // namespace multi_call
