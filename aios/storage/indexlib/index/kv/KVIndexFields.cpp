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
#include "indexlib/index/kv/KVIndexFields.h"

#include "autil/ConstString.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KVIndexFields);

KVIndexFields::KVIndexFields(autil::mem_pool::Pool* pool) : _fields(pool) { assert(pool); }

autil::StringView KVIndexFields::GetIndexType() const { return indexlib::index::KV_INDEX_TYPE_STR; }
size_t KVIndexFields::EstimateMemory() const
{
    // all fields is created from pooled allocator
    return 0;
}

const KVIndexFields::SingleField* KVIndexFields::GetSingleField(uint64_t indexNameHash) const
{
    auto iter = _fields.find(indexNameHash);
    if (iter == _fields.end()) {
        return nullptr;
    }
    return std::addressof(iter->second);
}

bool KVIndexFields::AddSingleField(uint64_t indexNameHash, const SingleField& field)
{
    auto [iter, inserted] = _fields.insert({indexNameHash, field});
    if (!inserted) {
        AUTIL_LOG(ERROR, "duplicate field [%lu], pk[%s]", indexNameHash, field.pkFieldValue.to_string().c_str());
    }
    return inserted;
}

void KVIndexFields::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(SERIALIZE_VERSION);
    uint32_t fieldNum = _fields.size();
    dataBuffer.write(fieldNum);
    for (const auto& [indexNameHash, field] : _fields) {
        dataBuffer.write(indexNameHash);
        dataBuffer.write(_opType);
        SerializeSingleField(dataBuffer, field);
    }
}

void KVIndexFields::deserialize(autil::DataBuffer& dataBuffer)
{
    uint32_t serializeVersion = 0;
    dataBuffer.read(serializeVersion);
    assert(serializeVersion == SERIALIZE_VERSION);
    uint32_t fieldNum = 0;
    dataBuffer.read(fieldNum);
    for (uint32_t i = 0; i < fieldNum; ++i) {
        uint64_t indexNameHash = 0;
        dataBuffer.read(indexNameHash);
        dataBuffer.read(_opType);
        SingleField singleField;
        DeserializeSingleField(dataBuffer, &singleField);
        if (!AddSingleField(indexNameHash, singleField)) {
            INDEXLIB_FATAL_ERROR(DocumentDeserialize, "add single field [%lu] failed", indexNameHash);
        }
    }
}

void KVIndexFields::DeserializeSingleField(autil::DataBuffer& dataBuffer, SingleField* singleField)
{
    dataBuffer.read(singleField->pkeyHash);
    dataBuffer.read(singleField->hasSkey);
    if (singleField->hasSkey) {
        dataBuffer.read(singleField->skeyHash);
    }
    dataBuffer.read(singleField->value);
    dataBuffer.read(singleField->ttl);
    dataBuffer.read(singleField->userTimestamp);
    dataBuffer.read(singleField->hasFormatError);
}

void KVIndexFields::SerializeSingleField(autil::DataBuffer& dataBuffer, const SingleField& singleField) const
{
    dataBuffer.write(singleField.pkeyHash);
    dataBuffer.write(singleField.hasSkey);
    if (singleField.hasSkey) {
        dataBuffer.write(singleField.skeyHash);
    }
    dataBuffer.write(singleField.value);
    dataBuffer.write(singleField.ttl);
    dataBuffer.write(singleField.userTimestamp);
    dataBuffer.write(singleField.hasFormatError);
}

DocOperateType KVIndexFields::GetDocOperateType() const { return _opType; }
void KVIndexFields::SetDocOperateType(DocOperateType type) { _opType = type; }

} // namespace indexlibv2::index
