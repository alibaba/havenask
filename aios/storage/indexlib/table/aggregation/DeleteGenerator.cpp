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
#include "indexlib/table/aggregation/DeleteGenerator.h"

#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/index/aggregation/AggregationIndexFields.h"
#include "indexlib/index/aggregation/AggregationKeyHasher.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/table/aggregation/AggregationSchemaUtil.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, DeleteGenerator);

DeleteGenerator::DeleteGenerator() = default;

DeleteGenerator::~DeleteGenerator()
{
    if (_deleteDataBuffer) {
        delete[] _deleteDataBuffer;
    }
}

Status DeleteGenerator::Init(const std::shared_ptr<config::TabletSchema>& schema,
                             const std::shared_ptr<index::PackAttributeFormatter>& packAttrFormatter)
{
    _packAttrFormatter = packAttrFormatter;

    auto indexConfigs = schema->GetIndexConfigs();
    for (size_t idx = 0; idx < indexConfigs.size(); ++idx) {
        const auto& indexConfig = indexConfigs[idx];
        if (index::AGGREGATION_INDEX_TYPE_STR != indexConfig->GetIndexType()) {
            continue;
        }
        auto aggIndexConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
        for (const auto& keyField : aggIndexConfig->GetKeyFields()) {
            auto ref = _packAttrFormatter->GetAttributeReference(keyField->GetFieldName());
            if (ref == nullptr) {
                return Status::InternalError("key field %s for index %s does not exist in primary key index",
                                             keyField->GetFieldName().c_str(), aggIndexConfig->GetIndexName().c_str());
            }
            _keyRefs.push_back(ref);
        }
        auto indexHash = config::IndexConfigHash::Hash(indexConfig);
        _aggIndexConfigs.emplace_back(indexHash, std::move(aggIndexConfig));
    }

    auto r = AggregationSchemaUtil::GetVersionField(schema.get());
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    auto versionFieldName = std::move(r.steal_value());
    _versionRef = _packAttrFormatter->GetAttributeReference(versionFieldName);
    if (!_versionRef) {
        return Status::InternalError("create AttributeReference for %s failed", versionFieldName.c_str());
    }

    r = AggregationSchemaUtil::GetUniqueField(schema.get());
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    auto uniqueFieldName = std::move(r.steal_value());
    _uniqueRef = _packAttrFormatter->GetAttributeReference(uniqueFieldName);
    if (!_uniqueRef) {
        return Status::InternalError("create AttributeReference for %s failed", uniqueFieldName.c_str());
    }
    return Status::OK();
}

Status DeleteGenerator::Generate(index::AggregationIndexFields* aggIndexFields, const autil::StringView& packedValue)
{
    auto uniqueValue = _uniqueRef->GetDataValue(packedValue.data()).valueStr;
    auto versionValue = _versionRef->GetDataValue(packedValue.data()).valueStr;
    size_t size = uniqueValue.size() + versionValue.size();

    if (_deleteDataSize != size) {
        if (_deleteDataBuffer != nullptr) {
            delete[] _deleteDataBuffer;
        }
        _deleteDataBuffer = new char[size];
        _deleteDataSize = size;
    }

    memcpy(_deleteDataBuffer, uniqueValue.data(), uniqueValue.size());
    memcpy(_deleteDataBuffer + uniqueValue.size(), versionValue.data(), versionValue.size());
    autil::StringView deletedValue(_deleteDataBuffer, size);

    size_t pos = 0;
    for (const auto& [indexNameHash, aggIndexConfig] : _aggIndexConfigs) {
        size_t keyCount = aggIndexConfig->GetKeyFields().size();
        auto r = CalculateHashKey(keyCount, packedValue, pos);
        if (!r.IsOK()) {
            return {r.steal_error()};
        }
        uint64_t pkeyHash = r.get();
        index::AggregationIndexFields::SingleField singleField {pkeyHash, deletedValue, DELETE_DOC, indexNameHash};
        aggIndexFields->PushFront(singleField);

        pos += keyCount;
    }
    return Status::OK();
}

static Status ExtractValue(const index::AttributeReference* ref, const autil::StringView& packedValue,
                           std::string& value)
{
    if (!ref->GetStrValue(packedValue.data(), value, nullptr)) {
        return Status::InternalError("extract value for %s failed", ref->GetAttrName().c_str());
    }
    return Status::OK();
}

StatusOr<uint64_t> DeleteGenerator::CalculateHashKey(size_t keyCount, const autil::StringView& packedValue,
                                                     size_t pos) const
{
    if (keyCount == 1) {
        std::string key;
        RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos], packedValue, key));
        return {index::AggregationKeyHasher::Hash(key)};
    } else if (keyCount == 2) {
        std::string key1, key2;
        RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos++], packedValue, key1));
        RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos++], packedValue, key2));
        return {index::AggregationKeyHasher::Hash(key1, key2)};
    } else if (keyCount == 3) {
        std::string key1, key2, key3;
        RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos++], packedValue, key1));
        RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos++], packedValue, key2));
        RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos++], packedValue, key3));
        return {index::AggregationKeyHasher::Hash(key1, key2, key3)};
    } else {
        std::vector<std::string> keys;
        keys.reserve(keyCount);
        for (size_t i = 0; i < keyCount; ++i) {
            std::string key;
            RETURN_STATUS_DIRECTLY_IF_ERROR(ExtractValue(_keyRefs[pos + i], packedValue, key));
            keys.emplace_back(std::move(key));
        }
        return {index::AggregationKeyHasher::Hash(keys)};
    }
}

} // namespace indexlibv2::table
