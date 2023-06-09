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
#include "indexlib/partition/modifier/patch_modifier.h"

#include <typeinfo>

#include "autil/ConstString.h"
#include "autil/EnvUtil.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/patch_attribute_modifier.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/patch_index_modifier.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader_interface.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/modifier/in_memory_segment_modifier.h"
#include "indexlib/partition/modifier/partition_modifier_task_item.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlibv2::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PatchModifier);

PatchModifier::PatchModifier(const IndexPartitionSchemaPtr& schema, bool enablePackFile, bool isOffline)
    : PartitionModifier(schema)
    , mBuildingSegmentBaseDocid(0)
    , mEnablePackFile(enablePackFile)
    , mIsOffline(isOffline)
{
}

PatchModifier::~PatchModifier() {}

void PatchModifier::Init(const PartitionDataPtr& partitionData,
                         const util::BuildResourceMetricsPtr& buildResourceMetrics)
{
    if (!buildResourceMetrics) {
        mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    } else {
        mBuildResourceMetrics = buildResourceMetrics;
    }

    mPkIndexReader = LoadPrimaryKeyIndexReader(partitionData);

    InitDeletionMapWriter(partitionData);
    InitAttributeModifier(partitionData);
    InitIndexModifier(partitionData);

    mBuildingSegmentBaseDocid = PartitionModifier::CalculateBuildingSegmentBaseDocId(partitionData);
    InitBuildingSegmentModifier(partitionData->GetInMemorySegment());

    mPartitionData = partitionData;
}

index::PartitionInfoPtr PatchModifier::GetPartitionInfo() const { return mPartitionData->GetPartitionInfo(); }

bool PatchModifier::UpdateDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    docid_t docId = doc->GetDocId();
    if (docId == INVALID_DOCID) {
        ERROR_COLLECTOR_LOG(ERROR, "target update document [pk:%s] is not exist!",
                            doc->GetIndexDocument()->GetPrimaryKey().c_str());
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        bool update = false;
        const auto& attrDoc = doc->GetAttributeDocument();
        if (mAttrModifier and attrDoc) {
            update = true;
            if (!mAttrModifier->Update(docId, attrDoc)) {
                return false;
            }
        }
        const auto& indexDoc = doc->GetIndexDocument();
        assert(indexDoc);
        if (mIndexModifier) {
            mIndexModifier->Update(docId, indexDoc);
            update = true;
        }
        return update;
    }

    if (mBuildingSegmentModifier) {
        docid_t localDocId = docId - mBuildingSegmentBaseDocid;
        return mBuildingSegmentModifier->UpdateDocument(localDocId, doc);
    }
    return false;
}

bool PatchModifier::UpdateField(docid_t docId, fieldid_t fieldId, const StringView& value, bool isNull)
{
    if (docId == INVALID_DOCID) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mAttrModifier->UpdateField(docId, fieldId, value, isNull);
    }

    // here not support update building segment
    return false;
}

bool PatchModifier::RedoIndex(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    if (!mIndexModifier) {
        IE_LOG(ERROR, "no index modifier");
        return false;
    }
    if (docId == INVALID_DOCID) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mIndexModifier->UpdateField(docId, modifiedTokens);
    }
    // here not support update building segment
    return false;
}

bool PatchModifier::DedupDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    doc->ModifyDocOperateType(DELETE_DOC);
    bool ret = RemoveDocument(doc);
    doc->ModifyDocOperateType(ADD_DOC);
    return ret;
}

bool PatchModifier::RemoveDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    docid_t docId = doc->GetDocId();
    return RemoveDocument(docId);
}

void PatchModifier::Dump(const DirectoryPtr& directory, segmentid_t srcSegmentId)
{
    if (!IsDirty()) {
        return;
    }

    if (mSchema->TTLEnabled()) {
        UpdateInMemorySegmentInfo();
    }

    mDeletionmapWriter->Dump(directory);
    if (mAttrModifier) {
        mAttrModifier->Dump(directory, srcSegmentId, mDumpThreadNum);
    }
    if (mIndexModifier) {
        mIndexModifier->Dump(directory, srcSegmentId, mDumpThreadNum);
    }
}

PrimaryKeyIndexReaderPtr PatchModifier::LoadPrimaryKeyIndexReader(const PartitionDataPtr& partitionData)
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema);
    IndexConfigPtr pkIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig) {
        return PrimaryKeyIndexReaderPtr();
    }
    PrimaryKeyIndexReaderPtr pkReader(
        IndexReaderFactory::CreatePrimaryKeyIndexReader(pkIndexConfig->GetInvertedIndexType()));
    assert(pkReader);
    bool needReverseLookup = autil::EnvUtil::getEnv("INDEXLIB_PATCH_MODIFIER_REVERSE_LOOKUP_PK", true);
    const auto& legacyPkReader = std::dynamic_pointer_cast<index::LegacyPrimaryKeyReaderInterface>(pkReader);
    assert(legacyPkReader);
    legacyPkReader->OpenWithoutPKAttribute(pkIndexConfig, partitionData, needReverseLookup);
    return pkReader;
}

void PatchModifier::InitDeletionMapWriter(const PartitionDataPtr& partitionData)
{
    mDeletionmapWriter.reset(new DeletionMapWriter(true));
    mDeletionmapWriter->Init(partitionData.get());
}

bool PatchModifier::IsDirty() const
{
    if (mDeletionmapWriter->IsDirty()) {
        return true;
    }

    if (mAttrModifier and mAttrModifier->IsDirty()) {
        return true;
    }
    if (mIndexModifier and mIndexModifier->IsDirty()) {
        return true;
    }

    return false;
}

bool PatchModifier::RemoveDocument(docid_t docId)
{
    if (docId == INVALID_DOCID) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mDeletionmapWriter->Delete(docId);
    }

    if (mBuildingSegmentModifier) {
        docid_t localDocId = docId - mBuildingSegmentBaseDocid;
        mBuildingSegmentModifier->RemoveDocument(localDocId);
        return true;
    }
    return false;
}

void PatchModifier::InitAttributeModifier(const PartitionDataPtr& partitionData)
{
    if (!mSchema->GetAttributeSchema()) {
        return;
    }

    mAttrModifier.reset(new PatchAttributeModifier(mSchema, mBuildResourceMetrics.get(), mIsOffline));
    mAttrModifier->Init(partitionData);
}

void PatchModifier::InitIndexModifier(const PartitionDataPtr& partitionData)
{
    if (!mSchema->GetIndexSchema() || !mSchema->GetIndexSchema()->AnyIndexUpdatable()) {
        return;
    }
    mIndexModifier.reset(new PatchIndexModifier(mSchema, mBuildResourceMetrics.get(), mIsOffline));
    mIndexModifier->Init(partitionData);
}

void PatchModifier::InitBuildingSegmentModifier(const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) {
        return;
    }
    mSegmentWriter = inMemSegment->GetSegmentWriter();
    mBuildingSegmentModifier = mSegmentWriter->GetInMemorySegmentModifier();
}

void PatchModifier::UpdateInMemorySegmentInfo()
{
    InMemorySegmentPtr inMemSegment = mPartitionData->GetInMemorySegment();
    if (!inMemSegment) {
        return;
    }

    SegmentInfoPtr segmentInfo = inMemSegment->GetSegmentWriter()->GetSegmentInfo();
    if (!segmentInfo) {
        return;
    }

    vector<segmentid_t> patchSegIds;
    mDeletionmapWriter->GetPatchedSegmentIds(patchSegIds);
    if (mAttrModifier) {
        mAttrModifier->GetPatchedSegmentIds(&patchSegIds);
    }

    sort(patchSegIds.begin(), patchSegIds.end());
    vector<segmentid_t>::iterator it = unique(patchSegIds.begin(), patchSegIds.end());
    patchSegIds.resize(distance(patchSegIds.begin(), it));
    for (size_t i = 0; i < patchSegIds.size(); ++i) {
        SegmentData segData = mPartitionData->GetSegmentData(patchSegIds[i]);
        segmentInfo->maxTTL = max(segmentInfo->maxTTL, segData.GetSegmentInfo()->maxTTL);
    }

    InMemorySegmentPtr subInMemSegment = inMemSegment->GetSubInMemorySegment();
    if (!subInMemSegment) {
        return;
    }
    SegmentInfoPtr subSegmentInfo = subInMemSegment->GetSegmentWriter()->GetSegmentInfo();
    if (!subSegmentInfo) {
        return;
    }
    segmentInfo->maxTTL = max(subSegmentInfo->maxTTL, segmentInfo->maxTTL);
}

PartitionModifierDumpTaskItemPtr PatchModifier::GetDumpTaskItem(const PartitionModifierPtr& modifier) const
{
    assert(dynamic_pointer_cast<PatchModifier>(modifier) != nullptr);
    PartitionModifierDumpTaskItemPtr taskItem(make_shared<PartitionModifierDumpTaskItem>());
    taskItem->taskType = PartitionModifierDumpTaskType::PatchModifierTask;
    taskItem->patchModifiers.emplace_back(modifier);
    taskItem->buildResourceMetrics = mBuildResourceMetrics;
    return taskItem;
}

bool PatchModifier::UpdatePackField(docid_t docId, packattrid_t packAttrId, const autil::StringView& value)
{
    assert(false);
    return false;
}

bool PatchModifier::UpdateIndex(IndexUpdateTermIterator* iterator)
{
    assert(false);
    return false;
}

}} // namespace indexlib::partition
