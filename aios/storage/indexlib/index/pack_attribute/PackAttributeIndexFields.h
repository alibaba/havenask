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
#include "autil/Log.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/IIndexFields.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/util/PooledContainer.h"

namespace indexlibv2::index {
class PackAttributeIndexFields : public document::IIndexFields
{
public:
    static constexpr uint32_t SERIALIZE_VERSION = 0;
    struct SingleField {
        autil::StringView value;
        bool isNull = false;
    };

public:
    explicit PackAttributeIndexFields(autil::mem_pool::Pool* pool);

public:
    void serialize(autil::DataBuffer& dataBuffer) const override;
    // caller should make sure inner data buffer of dataBuffer is alway valid before IndexFields deconstruct
    void deserialize(autil::DataBuffer& dataBuffer) override;
    size_t EstimateMemory() const override;
    bool HasFormatError() const override;
    autil::StringView GetIndexType() const override { return PACK_ATTRIBUTE_INDEX_TYPE_STR; }

public:
    void AddSingleField(const SingleField& field);
    void PushFront(const SingleField& field);
    auto begin() const { return _fields.begin(); }
    auto end() const { return _fields.end(); }
    void SetVersion(const autil::StringView& version);
    const autil::StringView& GetVersion() const;
    autil::mem_pool::Pool* GetPool() const { return _pool; }

private:
    indexlib::util::PooledVector<SingleField> _fields;
    autil::StringView _version;
    autil::mem_pool::Pool* _pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::index
