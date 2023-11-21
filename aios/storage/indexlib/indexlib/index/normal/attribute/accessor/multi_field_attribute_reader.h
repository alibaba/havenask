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
#include "indexlib/config/configurator_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlib { namespace index {

class MultiFieldAttributeReader
{
public:
    MultiFieldAttributeReader(const config::AttributeSchemaPtr& attributeSchema, AttributeMetrics* attributeMetrics,
                              config::ReadPreference readPreference, int32_t initReaderThreadCount);
    virtual ~MultiFieldAttributeReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData, const MultiFieldAttributeReader* hintReader = nullptr);

    const AttributeReaderPtr& GetAttributeReader(const std::string& field) const;
    void EnableAccessCountors(bool needReportTemperatureDetail);

    // for test
public:
    typedef std::unordered_map<std::string, AttributeReaderPtr> AttributeReaderMap;

    void AddAttributeReader(const std::string& attrName, const AttributeReaderPtr& attrReader);

    const AttributeReaderMap& GetAttributeReaders() const { return mAttrReaderMap; }

    void InitAttributeReader(const config::AttributeConfigPtr& attrConfig,
                             const index_base::PartitionDataPtr& partitionData,
                             const MultiFieldAttributeReader* hintReader = nullptr);

private:
    void InitAttributeReaders(const index_base::PartitionDataPtr& partitionData,
                              const MultiFieldAttributeReader* hintReader);
    void MultiThreadInitAttributeReaders(const index_base::PartitionDataPtr& partitionData, int32_t threadNum,
                                         const MultiFieldAttributeReader* hintReader);

private:
    config::AttributeSchemaPtr mAttrSchema;
    AttributeReaderMap mAttrReaderMap;
    mutable AttributeMetrics* mAttributeMetrics;
    mutable bool mEnableAccessCountors;
    config::ReadPreference mReadPreference;
    int32_t mInitReaderThreadCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldAttributeReader);
}} // namespace indexlib::index
