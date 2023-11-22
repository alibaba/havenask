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
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/IIndexFields.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/index/inverted_index/Types.h"
namespace indexlib::config {
class PayloadConfig;
}
namespace autil::mem_pool {
class Pool;
}
namespace indexlibv2::index {

class InvertedIndexFields : public document::IIndexFields
{
public:
    InvertedIndexFields(autil::mem_pool::Pool* pool);
    ~InvertedIndexFields();
    typedef std::vector<indexlib::document::Field*> FieldVector;

public:
    void serialize(autil::DataBuffer& dataBuffer) const override;
    void deserialize(autil::DataBuffer& dataBuffer) override;

    autil::StringView GetIndexType() const override;
    size_t EstimateMemory() const override;

    indexlib::document::Field* createIndexField(fieldid_t fieldId, indexlib::document::Field::FieldTag fieldTag);
    indexlib::document::Field* GetField(fieldid_t fieldId) const;
    termpayload_t GetTermPayload(uint64_t intKey) const;
    docpayload_t GetDocPayload(const indexlib::config::PayloadConfig& payloadConfig, uint64_t intKey) const;

private:
    static indexlib::document::Field* CreateFieldByTag(autil::mem_pool::Pool* pool,
                                                       indexlib::document::Field::FieldTag fieldTag, bool fieldHasPool);
    void SerializeFieldVector(autil::DataBuffer& dataBuffer, const FieldVector& fields) const;
    uint8_t GenerateFieldDescriptor(const indexlib::document::Field* field) const;
    indexlib::document::Field::FieldTag GetFieldTagFromFieldDescriptor(uint8_t fieldDescriptor);

    static constexpr uint32_t SERIALIZE_VERSION = 0;

private:
    FieldVector _fields; // need
    autil::mem_pool::Pool* _pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
