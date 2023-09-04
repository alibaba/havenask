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
#include "aios/network/gig/multi_call/util/ProtobufUtil.h"

using namespace google::protobuf;

namespace multi_call {

GrpcByteBufferSource::GrpcByteBufferSource() {
}

bool GrpcByteBufferSource::Init(const grpc::ByteBuffer &src) {
    size_ = src.Length();
    limit_ = size_;
    cur_ = -1;
    left_ = 0;
    ptr_ = nullptr;
    byte_count_ = 0;
    bool ok = src.Dump(&slices_).ok();
    if (!ok) {
        slices_.clear();
    }
    return ok;
}

bool GrpcByteBufferSource::Next(const void **data, int *size) {
    // Use loop instead of if in case buffer contained empty slices.
    if (byte_count_ >= limit_) {
        return false;
    }
    while (left_ == 0) {
        // Advance to next slice.
        cur_++;
        if (cur_ >= (int64_t)slices_.size()) {
            return false;
        }
        const ::grpc::Slice &s = slices_[cur_];
        left_ = s.size();
        ptr_ = reinterpret_cast<const char *>(s.begin());
    }
    int maxLength = limit_ - byte_count_;
    *data = ptr_;
    *size = left_;
    byte_count_ += left_;
    ptr_ += left_;
    left_ = 0;
    if (*size > maxLength) {
        BackUp(*size - maxLength);
        *size = maxLength;
    }
    return true;
}

void GrpcByteBufferSource::BackUp(int count) {
    ptr_ -= count;
    left_ += count;
    byte_count_ -= count;
}

bool GrpcByteBufferSource::Skip(int count) {
    const void *data;
    int size;
    while (Next(&data, &size)) {
        if (size >= count) {
            BackUp(size - count);
            return true;
        }
        // size < count;
        count -= size;
    }
    // error or we have too large count;
    return false;
}

grpc::protobuf::int64 GrpcByteBufferSource::ByteCount() const {
    return byte_count_;
}

void GrpcByteBufferSource::setLimit(int limit) {
    limit_ = limit;
}

int GrpcByteBufferSource::size() const {
    return size_;
}

GrpcByteBufferSourceByteReader::GrpcByteBufferSourceByteReader(GrpcByteBufferSource &source)
    : _source(source)
    , _data(nullptr)
    , _size(0) {
}

GrpcByteBufferSourceByteReader::~GrpcByteBufferSourceByteReader() {
    _source.BackUp(_size);
}

bool GrpcByteBufferSourceByteReader::next(uint8_t *byte) {
    while (_size <= 0) {
        if (!_source.Next(&_data, &_size)) {
            return false;
        }
    }
    uint8_t **typeBuffer = (uint8_t **)&_data;
    *byte = (*typeBuffer)[0];
    ++(*typeBuffer);
    _size -= 1;
    return true;
}

bool ProtobufCompatibleUtil::getFieldAndReflection(const google::protobuf::Message *message,
                                                   const std::string &fieldName,
                                                   FieldDescriptor::Type type,
                                                   const Reflection *&reflection,
                                                   const FieldDescriptor *&field) {
    if (!message) {
        return false;
    }
    reflection = message->GetReflection();
    const auto *descriptor = message->GetDescriptor();
    field = descriptor->FindFieldByName(fieldName);
    if (reflection && field && field->type() == type &&
        field->label() == FieldDescriptor::LABEL_OPTIONAL) {
        return true;
    }
    return false;
}

bool ProtobufCompatibleUtil::getErrorCodeField(const google::protobuf::Message *message,
                                               const std::string &fieldName,
                                               MultiCallErrorCode &ec) {
    const Reflection *reflection = nullptr;
    const FieldDescriptor *pbField = nullptr;
    if (getFieldAndReflection(message, fieldName, FieldDescriptor::TYPE_INT32, reflection,
                              pbField)) {
        if (reflection->HasField(*message, pbField)) {
            ec = (MultiCallErrorCode)reflection->GetInt32(*message, pbField);
            return true;
        }
    }
    return false;
}

void ProtobufCompatibleUtil::setErrorCodeField(google::protobuf::Message *message,
                                               const std::string &fieldName,
                                               MultiCallErrorCode ec) {
    const Reflection *reflection = nullptr;
    const FieldDescriptor *pbField = nullptr;
    if (getFieldAndReflection(message, fieldName, FieldDescriptor::TYPE_INT32, reflection,
                              pbField)) {
        reflection->SetInt32(message, pbField, ec);
    }
}

bool ProtobufCompatibleUtil::getGigMetaField(const google::protobuf::Message *message,
                                             const std::string &fieldName, std::string &meta) {
    const Reflection *reflection = nullptr;
    const FieldDescriptor *pbField = nullptr;
    if (getFieldAndReflection(message, fieldName, FieldDescriptor::TYPE_BYTES, reflection,
                              pbField)) {
        if (reflection->HasField(*message, pbField)) {
            meta = reflection->GetString(*message, pbField);
            return true;
        }
    }
    return false;
}

void ProtobufCompatibleUtil::setGigMetaField(google::protobuf::Message *message,
                                             const std::string &fieldName,
                                             const std::string &meta) {
    const Reflection *reflection = nullptr;
    const FieldDescriptor *pbField = nullptr;
    if (getFieldAndReflection(message, fieldName, FieldDescriptor::TYPE_BYTES, reflection,
                              pbField)) {
        reflection->SetString(message, pbField, meta);
    }
}

bool ProtobufCompatibleUtil::getStringField(const google::protobuf::Message *message,
                                            const std::string &fieldName, std::string &value) {
    const Reflection *reflection = nullptr;
    const FieldDescriptor *pbField = nullptr;
    if (getFieldAndReflection(message, fieldName, FieldDescriptor::TYPE_BYTES, reflection,
                              pbField)) {
        if (reflection->HasField(*message, pbField)) {
            value = reflection->GetString(*message, pbField);
            return true;
        }
    } else if (getFieldAndReflection(message, fieldName, FieldDescriptor::TYPE_STRING, reflection,
                                     pbField)) {
        if (reflection->HasField(*message, pbField)) {
            value = reflection->GetString(*message, pbField);
            return true;
        }
    }
    return false;
}

bool ProtobufCompatibleUtil::getEagleeyeField(const google::protobuf::Message *message,
                                              const std::string &traceIdField,
                                              const std::string &rpcIdField,
                                              const std::string &userDataField,
                                              std::string &traceId, std::string &rpcId,
                                              std::string &userDatas) {
    if (traceIdField.empty()) {
        return false;
    }

    if (!ProtobufCompatibleUtil::getStringField(message, traceIdField, traceId)) {
        return false;
    }

    if (traceId.empty()) {
        return false;
    }

    if (!rpcIdField.empty()) {
        ProtobufCompatibleUtil::getStringField(message, rpcIdField, rpcId);
    }

    if (!userDataField.empty()) {
        ProtobufCompatibleUtil::getStringField(message, userDataField, userDatas);
    }
    return true;
}

} // namespace multi_call
