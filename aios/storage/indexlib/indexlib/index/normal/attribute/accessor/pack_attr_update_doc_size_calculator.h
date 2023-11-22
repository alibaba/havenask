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

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

namespace indexlib { namespace index {

class PackAttrUpdateDocSizeCalculator
{
public:
    typedef std::map<segmentid_t, size_t> SegmentUpdateMap;

public:
    PackAttrUpdateDocSizeCalculator(const index_base::PartitionDataPtr& partitionData,
                                    const config::IndexPartitionSchemaPtr& schema)
        : mPartitionData(partitionData)
        , mSchema(schema)
    {
    }

    virtual ~PackAttrUpdateDocSizeCalculator() {}

public:
    virtual size_t EstimateUpdateDocSize(const index_base::Version& lastLoadVersion) const;
    size_t EstimateUpdateDocSizeInUnobseleteRtSeg(int64_t rtReclaimTimestamp) const;

    static size_t EsitmateOnePackAttributeUpdateDocSize(const config::PackAttributeConfigPtr& packAttrConfig,
                                                        const index_base::PartitionDataPtr& partitionData,
                                                        const index_base::Version& diffVersion);

    static size_t EstimateUpdateDocSizeInUpdateMap(const config::PackAttributeConfigPtr& packAttrConfig,
                                                   const SegmentUpdateMap& segUpdateMap,
                                                   const index_base::PartitionDataPtr& partitionData);

    static double GetAverageDocSize(const index_base::SegmentData& segData,
                                    const config::PackAttributeConfigPtr& packAttrConfig);
    static void ConstructUpdateMap(const std::string& packAttrName, const index_base::PartitionDataPtr& partitionData,
                                   const index_base::Version& diffVersion, SegmentUpdateMap& segUpdateMap);

private:
    bool HasUpdatablePackAttribute(const config::IndexPartitionSchemaPtr& schema) const;

    bool HasUpdatablePackAttrInAttrSchema(const config::AttributeSchemaPtr& attrSchema) const;

    size_t DoEstimateUpdateDocSize(const config::IndexPartitionSchemaPtr& schema,
                                   const index_base::PartitionDataPtr& partitionData,
                                   const index_base::Version& diffVersion) const;

    index_base::Version GenerateDiffVersionByTimestamp(const index_base::PartitionDataPtr& partitionData,
                                                       int64_t rtReclaimTimestamp) const;

private:
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;

private:
    friend class PackAttrUpdateDocSizeCalculatorTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttrUpdateDocSizeCalculator);
}} // namespace indexlib::index
