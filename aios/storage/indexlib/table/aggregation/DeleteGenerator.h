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

#include "autil/Log.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlibv2::index {
class AggregationIndexFields;
}

namespace indexlibv2::table {

class DeleteGenerator
{
public:
    DeleteGenerator();
    ~DeleteGenerator();

public:
    Status Init(const std::shared_ptr<config::TabletSchema>& schema,
                const std::shared_ptr<index::PackAttributeFormatter>& packAttrFormatter);
    Status Generate(index::AggregationIndexFields* aggIndexFields, const autil::StringView& packedValue);

private:
    StatusOr<uint64_t> CalculateHashKey(size_t keyCount, const autil::StringView& packedValue, size_t pos) const;

private:
    std::shared_ptr<index::PackAttributeFormatter> _packAttrFormatter;
    std::vector<const index::AttributeReference*> _keyRefs;
    const index::AttributeReference* _uniqueRef = nullptr;
    const index::AttributeReference* _versionRef = nullptr;
    std::vector<std::pair<size_t, std::shared_ptr<config::AggregationIndexConfig>>> _aggIndexConfigs;
    char* _deleteDataBuffer = nullptr;
    size_t _deleteDataSize = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
