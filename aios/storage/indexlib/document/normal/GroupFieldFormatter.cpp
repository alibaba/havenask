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
#include "indexlib/document/normal/GroupFieldFormatter.h"

#include "indexlib/util/buffer_compressor/ZlibDefaultCompressor.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, GroupFieldFormatter);

GroupFieldFormatter::GroupFieldFormatter() {}

void GroupFieldFormatter::Init(const string& compressType)
{
    if (compressType.empty()) {
        return;
    }
    _compressor.reset(BufferCompressorCreator::CreateBufferCompressor(compressType));
    if (!_compressor) {
        AUTIL_LOG(ERROR, "unsupported compressType[%s], use zlib_default instead", compressType.c_str());
        _compressor.reset(new ZlibDefaultCompressor());
    }
}

void GroupFieldFormatter::Reset()
{
    if (_compressor) {
        _compressor->Reset();
    }
}

GroupFieldFormatter::~GroupFieldFormatter() {}

std::pair<Status, size_t> GroupFieldFormatter::GetSerializeLength(GroupFieldIter& iter) const
{
    if (_compressor) {
        return GetSerializeLengthWithCompress(iter);
    }
    size_t len = 0;
    while (iter.HasNext()) {
        const StringView fieldValue = iter.Next();
        uint32_t valueLen = fieldValue.length();
        len += GetVUInt32Length(valueLen) + valueLen;
    }
    return {Status::OK(), len};
}

Status GroupFieldFormatter::SerializeFields(GroupFieldIter& iter, char* value, size_t length) const
{
    if (_compressor) {
        size_t comLen = _compressor->GetBufferOutLen();
        if (comLen > length) {
            RETURN_IF_STATUS_ERROR(Status::Corruption(), "The given buffer is too small! comLen[%lu] > length[%lu]",
                                   comLen, length);
        }
        memcpy(value, _compressor->GetBufferOut(), comLen);
        _compressor->Reset();
        return Status::OK();
    }

    // no compress, just [len,value] [len,value] ...
    char* cursor = value;
    char* end = value + length;
    while (iter.HasNext()) {
        const StringView fieldValue = iter.Next();
        uint32_t valueLen = fieldValue.length();
        if (unlikely(cursor + GetVUInt32Length(valueLen) + valueLen > end)) {
            RETURN_IF_STATUS_ERROR(Status::Corruption(), "The given buffer is too small!");
        }
        WriteVUInt32(valueLen, cursor);
        memcpy(cursor, fieldValue.data(), valueLen);
        cursor += valueLen;
    }
    return Status::OK();
}

std::pair<Status, size_t> GroupFieldFormatter::GetSerializeLengthWithCompress(GroupFieldIter& iter) const
{
    assert(_compressor != NULL);
    while (iter.HasNext()) {
        const StringView fieldValue = iter.Next();
        uint32_t valueLen = fieldValue.length();
        uint32_t vintLen = GetVUInt32Length(valueLen);
        uint64_t buffer;
        char* cursor = (char*)(&buffer);
        WriteVUInt32(valueLen, cursor);
        _compressor->AddDataToBufferIn((const char*)(&buffer), vintLen);
        _compressor->AddDataToBufferIn(fieldValue.data(), valueLen);
    }
    if (_compressor->GetBufferInLen() > 0 && !_compressor->Compress()) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), 0, "Compress Group FAILED.");
    }
    return {Status::OK(), _compressor->GetBufferOutLen()};
}

std::pair<Status, std::shared_ptr<SerializedGroupFieldIter>>
GroupFieldFormatter::Deserialize(const char* value, size_t length, autil::mem_pool::PoolBase* pool) const
{
    if (unlikely(value == NULL || length == 0)) {
        return {Status::OK(), nullptr};
    }
    size_t size = length;
    const char* outdata = value;
    if (_compressor) {
        _compressor->Reset();
        _compressor->AddDataToBufferIn(value, length);
        bool ret = _compressor->Decompress();
        if (!ret) {
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), nullptr, "Decompress GroupField FAILED.");
        }
        size = _compressor->GetBufferOutLen();
        outdata = _compressor->GetBufferOut();
    }
    if (pool != nullptr) {
        char* alocData = (char*)pool->allocate(size);
        if (alocData == nullptr) {
            return {Status::OK(), nullptr};
        }
        memcpy(alocData, outdata, size);
        outdata = alocData;
    }
    return {Status::OK(), std::shared_ptr<SerializedGroupFieldIter>(new SerializedGroupFieldIter(outdata, size))};
}

SerializedGroupFieldIter::SerializedGroupFieldIter(const char* value, size_t length)
    : _cursor(value)
    , _end(value + length)
    , _error(false)
{
}

SerializedGroupFieldIter::~SerializedGroupFieldIter() {}

bool SerializedGroupFieldIter::HasNext() const { return _cursor < _end; }

autil::StringView SerializedGroupFieldIter::Next()
{
    uint32_t valueLen = GroupFieldFormatter::ReadVUInt32(_cursor);
    if (likely(_cursor + valueLen <= _end)) {
        StringView ret(_cursor, valueLen);
        _cursor += valueLen;
        return ret;
    }
    _error = true;
    return StringView::empty_instance();
}

bool SerializedGroupFieldIter::HasError() const { return _error; }
}} // namespace indexlib::document
