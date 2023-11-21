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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace partition {

class PartitionIterator
{
public:
    PartitionIterator() {}
    ~PartitionIterator() {}

public:
    bool Init(const OfflinePartitionPtr& offlinePartition);

    index::AttributeDataIteratorPtr CreateSingleAttributeIterator(const std::string& attrName, segmentid_t segmentId);

    bool GetSegmentDocCount(segmentid_t segmentId, uint32_t& docCount) const;

    config::IndexPartitionSchemaPtr GetSchema() const { return mSchema; }

private:
    config::AttributeConfigPtr GetAttributeConfig(const std::string& attrName);

private:
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionIterator);
}} // namespace indexlib::partition
