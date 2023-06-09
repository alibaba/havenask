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
#include "indexlib/partition/remote_access/partition_iterator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/single_value_data_iterator.h"
#include "indexlib/index/normal/attribute/accessor/var_num_data_iterator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/offline_partition.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionIterator);

bool PartitionIterator::Init(const OfflinePartitionPtr& partition)
{
    if (!partition) {
        return false;
    }

    TableType type = partition->GetSchema()->GetTableType();
    if (type != tt_index) {
        IE_LOG(ERROR, "only support index table!");
        return false;
    }

    mPartitionData = partition->GetPartitionData();
    mSchema = partition->GetSchema();
    return true;
}

bool PartitionIterator::GetSegmentDocCount(segmentid_t segmentId, uint32_t& docCount) const
{
    Version version = mPartitionData->GetOnDiskVersion();
    if (!version.HasSegment(segmentId)) {
        return false;
    }
    SegmentData segData = mPartitionData->GetSegmentData(segmentId);
    docCount = segData.GetSegmentInfo()->docCount;
    return true;
}

AttributeDataIteratorPtr PartitionIterator::CreateSingleAttributeIterator(const string& attrName, segmentid_t segmentId)
{
    IE_LOG(INFO, "create attribute iterator for attribute [%s] in segment [%d]", attrName.c_str(), segmentId);
    Version version = mPartitionData->GetOnDiskVersion();
    if (!version.HasSegment(segmentId)) {
        IE_LOG(ERROR, "segment [%d] not in version [%d]", segmentId, version.GetVersionId());
        return AttributeDataIteratorPtr();
    }

    AttributeConfigPtr attrConf = GetAttributeConfig(attrName);
    if (!attrConf) {
        return AttributeDataIteratorPtr();
    }

    AttributeDataIteratorPtr iterator;
    FieldType ft = attrConf->GetFieldType();
    bool isMultiValue = attrConf->IsMultiValue();
    switch (ft) {
#define MACRO(field_type)                                                                                              \
    case field_type: {                                                                                                 \
        if (!isMultiValue) {                                                                                           \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            iterator.reset(new SingleValueDataIterator<T>(attrConf));                                                  \
        } else {                                                                                                       \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            iterator.reset(new VarNumDataIterator<T>(attrConf));                                                       \
        }                                                                                                              \
        break;                                                                                                         \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO

    case ft_string:
        if (!isMultiValue) {
            iterator.reset(new VarNumDataIterator<char>(attrConf));
        } else {
            iterator.reset(new VarNumDataIterator<autil::MultiChar>(attrConf));
        }
        break;
    default:
        assert(false);
    }

    if (!iterator || !iterator->Init(mPartitionData, segmentId)) {
        return AttributeDataIteratorPtr();
    }
    return iterator;
}

AttributeConfigPtr PartitionIterator::GetAttributeConfig(const string& attrName)
{
    if (!mSchema) {
        return AttributeConfigPtr();
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        IE_LOG(ERROR, "no attribute schema!");
        return AttributeConfigPtr();
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrName);
    if (!attrConfig) {
        IE_LOG(ERROR, "attribute [%s] not found!", attrName.c_str());
        return AttributeConfigPtr();
    }

    if (attrConfig->GetPackAttributeConfig() != NULL) {
        IE_LOG(ERROR, "attribute [%s] in pack attribute, not support!", attrName.c_str());
        return AttributeConfigPtr();
    }
    return attrConfig;
}
}} // namespace indexlib::partition
