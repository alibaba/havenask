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
#include "indexlib/index/normal/inverted_index/accessor/patch_index_modifier.h"

#include "indexlib/common/dump_thread_pool.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PatchIndexModifier);

PatchIndexModifier::PatchIndexModifier(const IndexPartitionSchemaPtr& schema,
                                       util::BuildResourceMetrics* buildResourceMetrics, bool isOffline)
    : IndexModifier(schema, buildResourceMetrics)
    , _isOffline(isOffline)
    , _enableCounters(false)
{
}

PatchIndexModifier::~PatchIndexModifier() {}

void PatchIndexModifier::Init(const index_base::PartitionDataPtr& partitionData)
{
    _segmentModifierContainer.Init(partitionData, [this](segmentid_t segId) mutable {
        return IndexSegmentModifierPtr(
            new IndexSegmentModifier(segId, _schema->GetIndexSchema(), _buildResourceMetrics));
    });
    InitCounters(partitionData->GetCounterMap());
}

void PatchIndexModifier::InitCounters(const util::CounterMapPtr& counterMap)
{
    // TODO(hanyao): index update counter
    //     if (!mIsOffline || !counterMap)
    //     {
    //         mEnableCounters = false;
    //         return;
    //     }

    //     AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    //     assert(attrSchema);
    //     uint32_t count = attrSchema->GetAttributeCount();
    //     mAttrIdToCounters.resize(count);
    //     string counterPrefix = "offline.updateField.";

    //     for (uint32_t i = 0; i < count; ++i)
    //     {
    //         AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(i);
    //         if (!attrConfig)
    //         {
    //             continue;
    //         }

    //         string counterName = counterPrefix + attrConfig->GetAttrName();
    //         auto counter = counterMap->GetAccCounter(counterName);
    //         if (!counter)
    //         {
    //             INDEXLIB_FATAL_ERROR(InconsistentState,
    //                     "init counter[%s] failed", counterName.c_str());
    //         }
    //         mAttrIdToCounters[i] = counter;
    //     }
    //     mEnableCounters = true;
}

bool PatchIndexModifier::UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    docid_t localId = INVALID_DOCID;
    const auto& segModifier = _segmentModifierContainer.GetSegmentModifier(docId, localId);
    assert(segModifier);
    fieldid_t fieldId = modifiedTokens.FieldId();
    const std::vector<indexid_t>& indexIdList = _schema->GetIndexSchema()->GetIndexIdList(fieldId);
    for (indexid_t indexId : indexIdList) {
        const auto& indexConfig = _schema->GetIndexSchema()->GetIndexConfig(indexId);
        if (!indexConfig || indexConfig->IsDisable() || indexConfig->IsDeleted()) {
            continue;
        }
        if (!indexConfig->GetNonTruncateIndexName().empty()) {
            continue;
        }
        if (!indexConfig->IsIndexUpdatable()) {
            assert(false);
        }
        segModifier->Update(localId, indexId, modifiedTokens);
    }
    return true;
}

void PatchIndexModifier::Update(docid_t docId, const document::IndexDocumentPtr& indexDoc)
{
    const auto& modifiedTokens = indexDoc->GetModifiedTokens();
    if (modifiedTokens.empty()) {
        return;
    }
    for (const auto& modifiedToken : modifiedTokens) {
        UpdateField(docId, modifiedToken);
    }
}

void PatchIndexModifier::Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegmentId, uint32_t dumpThreadNum)
{
    if (!IsDirty()) {
        return;
    }

    file_system::DirectoryPtr indexDir = dir->MakeDirectory(INDEX_DIR_NAME);
    _segmentModifierContainer.Dump(indexDir, srcSegmentId, dumpThreadNum, "indexDumpPatch");
}
}} // namespace indexlib::index
