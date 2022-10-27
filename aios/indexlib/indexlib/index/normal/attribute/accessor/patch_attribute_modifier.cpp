#include "indexlib/index/normal/attribute/accessor/patch_attribute_modifier.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/index/normal/attribute/accessor/built_attribute_segment_modifier.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/dump_thread_pool.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PatchAttributeModifier);

PatchAttributeModifier::PatchAttributeModifier(
        const IndexPartitionSchemaPtr& schema,
        util::BuildResourceMetrics* buildResourceMetrics,
        bool isOffline)
    : AttributeModifier(schema, buildResourceMetrics)
    , mIsOffline(isOffline)
    , mEnableCounters(false) {}

PatchAttributeModifier::~PatchAttributeModifier() 
{
}

void PatchAttributeModifier::Init(const index_base::PartitionDataPtr& partitionData)
{
    PartitionData::Iterator iter = partitionData->Begin();
    docid_t docCount = 0;
    for ( ; iter != partitionData->End(); iter++)
    {
        const SegmentData& segmentData = *iter;
        segmentid_t segmentId = segmentData.GetSegmentId();
        docid_t baseDocId = segmentData.GetBaseDocId();
        docCount += segmentData.GetSegmentInfo().docCount;
        mDumpedSegmentBaseIdVect.push_back(
                SegmentBaseDocIdInfo(segmentId, baseDocId));
    }
    InitPackAttributeUpdateBitmap(partitionData);
    InitCounters(partitionData->GetCounterMap());
}

void PatchAttributeModifier::InitCounters(const util::CounterMapPtr& counterMap)
{
    if (!mIsOffline || !counterMap)
    {
        mEnableCounters = false;
        return;
    }

    
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    assert(attrSchema);
    uint32_t count = attrSchema->GetAttributeCount();
    mAttrIdToCounters.resize(count);
    string counterPrefix = "offline.updateField.";
    
    for (uint32_t i = 0; i < count; ++i)
    {
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(i);
        if (!attrConfig)
        {
            continue;
        }
        
        string counterName = counterPrefix + attrConfig->GetAttrName();
        auto counter = counterMap->GetAccCounter(counterName);
        if (!counter)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "init counter[%s] failed", counterName.c_str());
        }
        mAttrIdToCounters[i] = counter;
    }
    mEnableCounters = true;
}

bool PatchAttributeModifier::UpdateField(
        docid_t docId, fieldid_t fieldId, const ConstString& value)
{
    docid_t localId = INVALID_DOCID;
    BuiltAttributeSegmentModifierPtr segModifier = 
        GetSegmentModifier(docId, localId);
    assert(segModifier);
    const AttributeConfigPtr& attrConfig = 
        mSchema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId);

    if (!attrConfig)
    {
        return false;
    }

    segModifier->Update(localId, attrConfig->GetAttrId(), value);
    return true;    
}

bool PatchAttributeModifier::Update(docid_t docId, 
                                    const AttributeDocumentPtr& attrDoc)
{
    UpdateFieldExtractor extractor(mSchema);
    if (!extractor.Init(attrDoc))
    {
        return false;
    }

    if (extractor.GetFieldCount() == 0)
    {
        return true;
    }

    docid_t localId = INVALID_DOCID;
    BuiltAttributeSegmentModifierPtr segModifier = GetSegmentModifier(
            docId, localId);
    assert(segModifier);

    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext())
    {
        fieldid_t fieldId = INVALID_FIELDID;
        const ConstString& value = iter.Next(fieldId);

        const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[fieldId];
        assert(convertor);
        const AttributeConfigPtr& attrConfig = 
            mSchema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId);
        assert(attrConfig);

        if (mEnableCounters)
        {
            mAttrIdToCounters[attrConfig->GetAttrId()]->Increase(1);
        }

        const common::AttrValueMeta& meta = convertor->Decode(value);
        segModifier->Update(localId, attrConfig->GetAttrId(), meta.data);

        PackAttributeConfig* packAttrConfig =
            attrConfig->GetPackAttributeConfig();
        
        if (packAttrConfig)
        {
            const AttributeUpdateBitmapPtr& packAttrUpdateBitmap =
                mPackUpdateBitmapVec[packAttrConfig->GetPackAttrId()];
            if (packAttrUpdateBitmap)
            {
                packAttrUpdateBitmap->Set(docId);
            }
        }
    }
    
    return true;
}

BuiltAttributeSegmentModifierPtr PatchAttributeModifier::CreateBuiltSegmentModifier(
        segmentid_t segmentId)
{
    return BuiltAttributeSegmentModifierPtr(
            new BuiltAttributeSegmentModifier(segmentId, 
                    mSchema->GetAttributeSchema(), mBuildResourceMetrics));
}

void PatchAttributeModifier::Dump(const file_system::DirectoryPtr& dir,
                                  segmentid_t srcSegmentId, uint32_t dumpThreadNum)
{
    if (!IsDirty())
    {
        return;
    }

    file_system::DirectoryPtr attrDir = dir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    BuiltSegmentModifierMap::const_iterator it = mSegId2BuiltModifierMap.begin();
    if (dumpThreadNum > 1)
    {
        IE_LOG(INFO, "MultiThreadDumpPathFile with dump thred num [%u] begin!", dumpThreadNum);
        DumpThreadPool dumpThreadPool(dumpThreadNum, 1024);
        if (!dumpThreadPool.start("indexDumpPatch")) {
            INDEXLIB_THROW(misc::RuntimeException, "create dump thread pool failed");
        }
        vector<common::DumpItem*> dumpItems;
        for ( ; it != mSegId2BuiltModifierMap.end(); ++it)
        {
            it->second->CreateDumpItems(attrDir, srcSegmentId, dumpItems);
        }
        for (size_t i = 0; i < dumpItems.size(); ++i)
        {
            dumpThreadPool.pushWorkItem(dumpItems[i]);
        }
        dumpThreadPool.stop();
        IE_LOG(INFO, "MultiThreadDumpPathFile end, dumpMemoryPeak [%lu]",
               dumpThreadPool.getPeakOfMemoryUse());
    }
    else
    {
        for ( ; it != mSegId2BuiltModifierMap.end(); ++it)
        {
            it->second->Dump(attrDir, srcSegmentId);
        }
    }
    DumpPackAttributeUpdateInfo(attrDir);
}

void PatchAttributeModifier::GetPatchedSegmentIds(
        vector<segmentid_t> &patchSegIds) const
{
    BuiltSegmentModifierMap::const_iterator it = 
        mSegId2BuiltModifierMap.begin();
    for ( ; it != mSegId2BuiltModifierMap.end(); ++it)
    {
        patchSegIds.push_back(it->first);
    }
}

IE_NAMESPACE_END(index);

