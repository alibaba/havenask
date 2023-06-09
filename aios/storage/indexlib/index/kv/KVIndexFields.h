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

#include <unordered_map>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/StringView.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/IIndexFields.h"
#include "indexlib/util/PooledContainer.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {

class KVIndexFields : public document::IIndexFields
{
public:
    static constexpr uint32_t SERIALIZE_VERSION = 0;

public:
    struct SingleField {
        uint64_t pkeyHash = 0;
        uint64_t skeyHash = 0;
        bool hasSkey = false;
        autil::StringView value;
        uint32_t ttl = UNINITIALIZED_TTL;
        int64_t userTimestamp = INVALID_TIMESTAMP;
        bool hasFormatError = false;

        // do not serialize/deserialize
        autil::StringView pkFieldName;
        autil::StringView pkFieldValue;
    };

public:
    explicit KVIndexFields(autil::mem_pool::Pool* pool);
    ~KVIndexFields() = default;

    void serialize(autil::DataBuffer& dataBuffer) const override;
    // caller should make sure inner data buffer of dataBuffer is alway valid before IndexFields deconstruct
    void deserialize(autil::DataBuffer& dataBuffer) override;

    autil::StringView GetIndexType() const override;
    size_t EstimateMemory() const override;
    bool HasFormatError() const override;

public:
    const SingleField* GetSingleField(uint64_t indexNameHash) const;
    bool AddSingleField(uint64_t indexNameHash, const SingleField& field);
    DocOperateType GetDocOperateType() const;
    void SetDocOperateType(DocOperateType type);

    auto begin() const { return _fields.begin(); }
    auto end() const { return _fields.end(); }

private:
    void DeserializeSingleField(autil::DataBuffer& dataBuffer, SingleField* singleField);
    void SerializeSingleField(autil::DataBuffer& dataBuffer, const SingleField& singleField) const;

private:
    indexlib::util::PooledUnorderedMap<uint64_t, SingleField> _fields;
    DocOperateType _opType = ADD_DOC;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
