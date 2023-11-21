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
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class MultiPackAttributeReader
{
public:
    MultiPackAttributeReader(const config::AttributeSchemaPtr& attributeSchema,
                             AttributeMetrics* attributeMetrics = NULL);

    ~MultiPackAttributeReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData, const MultiPackAttributeReader* hintReader = nullptr);
    const PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const;
    const PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const;

private:
    typedef std::unordered_map<std::string, size_t> NameToIdxMap;
    config::AttributeSchemaPtr mAttrSchema;
    AttributeMetrics* mAttributeMetrics;
    std::vector<PackAttributeReaderPtr> mPackAttrReaders;
    NameToIdxMap mPackNameToIdxMap;
    static PackAttributeReaderPtr RET_EMPTY_PTR;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPackAttributeReader);
}} // namespace indexlib::index
