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
#include "indexlib/index/aggregation/AggregationIndexFields.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AggregationIndexFields);

AggregationIndexFields::AggregationIndexFields(autil::mem_pool::Pool* pool) : _fields(pool), _pool(pool)
{
    assert(pool);
}

void AggregationIndexFields::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(SERIALIZE_VERSION);
    dataBuffer.write(_version);
    uint32_t fieldNum = _fields.size();
    dataBuffer.write(fieldNum);
    for (const auto& field : _fields) {
        dataBuffer.write(field.pkeyHash);
        dataBuffer.write(field.value);
        dataBuffer.write(field.opType);
        dataBuffer.write(field.indexNameHash);
    }
}
// caller should make sure inner data buffer of dataBuffer is alway valid before IndexFields deconstruct
void AggregationIndexFields::deserialize(autil::DataBuffer& dataBuffer)
{
    assert(_pool->isInPool(dataBuffer.getData()));
    _fields.clear();
    uint32_t serializeVersion = 0;
    dataBuffer.read(serializeVersion);
    assert(serializeVersion == SERIALIZE_VERSION);
    dataBuffer.read(_version);
    uint32_t fieldNum = 0;
    dataBuffer.read(fieldNum);
    _fields.reserve(fieldNum);
    for (uint32_t i = 0; i < fieldNum; ++i) {
        SingleField field;
        dataBuffer.read(field.pkeyHash);
        dataBuffer.read(field.value);
        dataBuffer.read(field.opType);
        dataBuffer.read(field.indexNameHash);
        _fields.push_back(field);
    }
}

size_t AggregationIndexFields::EstimateMemory() const
{
    // all fields is created from pooled allocator
    return 0;
}
bool AggregationIndexFields::HasFormatError() const { return false; }

void AggregationIndexFields::AddSingleField(const SingleField& field) { _fields.push_back(field); }

void AggregationIndexFields::PushFront(const SingleField& field) { _fields.insert(_fields.begin(), field); }

void AggregationIndexFields::SetVersion(const autil::StringView& version) { _version = version; }

const autil::StringView& AggregationIndexFields::GetVersion() const { return _version; }

} // namespace indexlibv2::index
