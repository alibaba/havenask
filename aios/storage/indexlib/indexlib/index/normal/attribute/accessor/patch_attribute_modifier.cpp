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
#include "indexlib/index/normal/attribute/accessor/patch_attribute_modifier.h"

#include "indexlib/common/dump_thread_pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/index/normal/attribute/accessor/built_attribute_segment_modifier.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
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
IE_LOG_SETUP(index, PatchAttributeModifier);

PatchAttributeModifier::PatchAttributeModifier(const IndexPartitionSchemaPtr& schema,
                                               util::BuildResourceMetrics* buildResourceMetrics, bool isOffline)
    : AttributeModifier(schema, buildResourceMetrics)
    , mIsOffline(isOffline)
    , mEnableCounters(false)
{
}

PatchAttributeModifier::~PatchAttributeModifier() {}

void PatchAttributeModifier::Init(const index_base::PartitionDataPtr& partitionData)
{
    mSegmentModifierContainer.Init(partitionData, [this](segmentid_t segId) mutable {
        return BuiltAttributeSegmentModifierPtr(
            new BuiltAttributeSegmentModifier(segId, mSchema->GetAttributeSchema(), mBuildResourceMetrics));
    });
    InitPackAttributeUpdateBitmap(partitionData);
    InitCounters(partitionData->GetCounterMap());
}

void PatchAttributeModifier::InitCounters(const util::CounterMapPtr& counterMap)
{
    if (!mIsOffline || !counterMap) {
        mEnableCounters = false;
        return;
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    assert(attrSchema);
    uint32_t count = attrSchema->GetAttributeCount();
    mAttrIdToCounters.resize(count);
    string counterPrefix = "offline.updateField.";

    for (uint32_t i = 0; i < count; ++i) {
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(i);
        if (!attrConfig) {
            continue;
        }

        string counterName = counterPrefix + attrConfig->GetAttrName();
        auto counter = counterMap->GetAccCounter(counterName);
        if (!counter) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterName.c_str());
        }
        mAttrIdToCounters[i] = counter;
    }
    mEnableCounters = true;
}

bool PatchAttributeModifier::UpdateField(docid_t docId, fieldid_t fieldId, const StringView& value, bool isNull)
{
    docid_t localId = INVALID_DOCID;
    BuiltAttributeSegmentModifierPtr segModifier = mSegmentModifierContainer.GetSegmentModifier(docId, localId);
    assert(segModifier);
    const AttributeConfigPtr& attrConfig = mSchema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId);

    if (!attrConfig) {
        return false;
    }

    segModifier->Update(localId, attrConfig->GetAttrId(), value, isNull);
    return true;
}

bool PatchAttributeModifier::Update(docid_t docId, const AttributeDocumentPtr& attrDoc)
{
    UpdateFieldExtractor extractor(mSchema);
    if (!extractor.Init(attrDoc)) {
        return false;
    }

    if (extractor.GetFieldCount() == 0) {
        return true;
    }

    docid_t localId = INVALID_DOCID;
    BuiltAttributeSegmentModifierPtr segModifier = mSegmentModifierContainer.GetSegmentModifier(docId, localId);
    assert(segModifier);

    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext()) {
        fieldid_t fieldId = INVALID_FIELDID;
        bool isNull = false;
        StringView value = iter.Next(fieldId, isNull);
        const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[fieldId];
        assert(convertor);
        const AttributeConfigPtr& attrConfig = mSchema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId);
        assert(attrConfig);

        if (mEnableCounters) {
            mAttrIdToCounters[attrConfig->GetAttrId()]->Increase(1);
        }

        if (!isNull) {
            common::AttrValueMeta meta = convertor->Decode(value);
            segModifier->Update(localId, attrConfig->GetAttrId(), meta.data, false);
        } else {
            segModifier->Update(localId, attrConfig->GetAttrId(), StringView::empty_instance(), true);
        }

        PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();

        if (packAttrConfig) {
            const AttributeUpdateBitmapPtr& packAttrUpdateBitmap =
                mPackUpdateBitmapVec[packAttrConfig->GetPackAttrId()];
            if (packAttrUpdateBitmap) {
                packAttrUpdateBitmap->Set(docId);
            }
        }
    }

    return true;
}

void PatchAttributeModifier::Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegmentId,
                                  uint32_t dumpThreadNum)
{
    if (!IsDirty()) {
        return;
    }

    file_system::DirectoryPtr attrDir = dir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    mSegmentModifierContainer.Dump(attrDir, srcSegmentId, dumpThreadNum, "attrDumpPatch");
    DumpPackAttributeUpdateInfo(attrDir);
}
}} // namespace indexlib::index
