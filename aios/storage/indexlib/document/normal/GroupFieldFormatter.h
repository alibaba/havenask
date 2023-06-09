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
#pragma once
#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/normal/GroupFieldIter.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"

namespace indexlib { namespace document {

class SerializedGroupFieldIter
{
public:
    SerializedGroupFieldIter(const char* value, size_t length);
    ~SerializedGroupFieldIter();

public:
    bool HasNext() const;
    autil::StringView Next();
    bool HasError() const;

private:
    const char* _cursor;
    const char* _end;
    bool _error;

private:
    AUTIL_LOG_DECLARE();
};

class GroupFieldFormatter
{
public:
    GroupFieldFormatter();
    ~GroupFieldFormatter();

public:
    void Init(const std::string& compressType);
    void Reset();
    std::pair<Status, size_t> GetSerializeLength(GroupFieldIter& iter) const;
    Status SerializeFields(GroupFieldIter& iter, char* value, size_t length) const;
    std::pair<Status, std::shared_ptr<SerializedGroupFieldIter>>
    Deserialize(const char* value, size_t length, autil::mem_pool::PoolBase* pool = nullptr) const;

public:
    static uint32_t GetVUInt32Length(uint32_t value);
    static void WriteVUInt32(uint32_t value, char*& cursor);
    static uint32_t ReadVUInt32(const char*& cursor);

private:
    std::pair<Status, size_t> GetSerializeLengthWithCompress(GroupFieldIter& iter) const;

private:
    util::BufferCompressorPtr _compressor;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////
inline uint32_t GroupFieldFormatter::GetVUInt32Length(uint32_t value)
{
    uint32_t len = 1;
    while (value > 0x7F) {
        ++len;
        value >>= 7;
    }
    return len;
}

inline void GroupFieldFormatter::WriteVUInt32(uint32_t value, char*& cursor)
{
    while (value > 0x7F) {
        *cursor++ = 0x80 | (value & 0x7F);
        value >>= 7;
    }
    *cursor++ = value & 0x7F;
}

inline uint32_t GroupFieldFormatter::ReadVUInt32(const char*& cursor)
{
    char byte = *cursor++;
    uint32_t value = byte & 0x7F;
    int shift = 7;
    while (byte & 0x80) {
        byte = *cursor++;
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return value;
}
}} // namespace indexlib::document
