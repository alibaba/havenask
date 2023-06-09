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
#include "build_service/custom_merger/ReuseAttributeAlterFieldMerger.h"

#include "build_service/custom_merger/MergeResourceProvider.h"

using namespace std;
using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::index;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, ReuseAttributeAlterFieldMerger);

const std::string ReuseAttributeAlterFieldMerger::MERGER_NAME = "ReuseAttributeAlterFieldMerger";

ReuseAttributeAlterFieldMerger::ReuseAttributeAlterFieldMerger(uint32_t backupId, const string& epochId)
    : AlterDefaultFieldMerger::AlterDefaultFieldMerger(backupId, epochId)
{
}

ReuseAttributeAlterFieldMerger::~ReuseAttributeAlterFieldMerger() {}

void ReuseAttributeAlterFieldMerger::FillAttributeValue(const PartitionIteratorPtr& partDataIter,
                                                        const string& attrName, segmentid_t segmentId,
                                                        const AttributeDataPatcherPtr& attrPatcher)
{
    AttributeDataIteratorPtr attrIter = partDataIter->CreateSingleAttributeIterator(attrName, segmentId);
    if (!attrIter) {
        return;
    }

    bool appendFromString = true;
    AttributeConfigPtr oldConfig = attrIter->GetAttrConfig();
    AttributeConfigPtr newConfig = attrPatcher->GetAttrConfig();
    if (oldConfig->GetFieldType() == newConfig->GetFieldType() &&
        oldConfig->IsMultiValue() == newConfig->IsMultiValue() &&
        oldConfig->GetCompressType() == newConfig->GetCompressType() &&
        oldConfig->GetFieldConfig()->GetFixedMultiValueCount() ==
            newConfig->GetFieldConfig()->GetFixedMultiValueCount()) {
        appendFromString = false; // from raw index
        BS_LOG(INFO, "append from legacy attribute [%s] with raw index data", attrName.c_str());
    } else {
        BS_LOG(INFO, "append from legacy attribute [%s] with literal string", attrName.c_str());
    }

    int interval = (int)attrPatcher->GetTotalDocCount() / 100;
    while (attrIter->IsValid()) {
        if (appendFromString) {
            attrPatcher->AppendFieldValue(attrIter->GetValueStr());
        } else {
            attrPatcher->AppendFieldValueFromRawIndex(attrIter->GetRawIndexImageValue());
        }
        attrIter->MoveToNext();
        if (interval > 0) {
            BS_INTERVAL_LOG(interval, INFO,
                            "append attribute value for segment %d "
                            "from legacy attribute [%s], done %u, total %u",
                            segmentId, attrName.c_str(), attrPatcher->GetPatchedDocCount(),
                            attrPatcher->GetTotalDocCount());
        }
    }
}

}} // namespace build_service::custom_merger
