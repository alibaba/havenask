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
#include "indexlib/partition/modifier/inplace_modifier.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/inplace_index_modifier.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/modifier/partition_modifier_task_item.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InplaceModifier);

InplaceModifier::InplaceModifier(const IndexPartitionSchemaPtr& schema)
    : PartitionModifier(schema)
    , mRealtimeSegmentBaseDocId(0)
    , mBuildingSegmentBaseDocid(0)
    , mBuildExecutor(nullptr)
{
}

InplaceModifier::~InplaceModifier() {}

void InplaceModifier::Init(const IndexPartitionReaderPtr& partitionReader, const InMemorySegmentPtr& inMemSegment,
                           const util::BuildResourceMetricsPtr& buildResourceMetrics)
{
    mPartitionReader = partitionReader;
    mPkIndexReader = partitionReader->GetPrimaryKeyReader();
    mBuildExecutor = mPkIndexReader->GetBuildExecutor();
    mDeletionmapReader = partitionReader->GetDeletionMapReader();
    if (!buildResourceMetrics) {
        mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    } else {
        mBuildResourceMetrics = buildResourceMetrics;
    }

    if (mSchema->GetAttributeSchema()) {
        mAttrModifier.reset(new InplaceAttributeModifier(mSchema, mBuildResourceMetrics.get()));
        mAttrModifier->Init(partitionReader->GetAttributeReaderContainer(), partitionReader->GetPartitionData());
    }

    if (mSchema->GetIndexSchema() && mSchema->GetIndexSchema()->AnyIndexUpdatable()) {
        mIndexModifier.reset(new InplaceIndexModifier(mSchema, mBuildResourceMetrics.get()));
        mIndexModifier->Init(partitionReader->GetInvertedIndexReader(), partitionReader->GetPartitionData());
    }

    mRealtimeSegmentBaseDocId = CalculateRealtimeSegmentBaseDocId(partitionReader->GetPartitionData());
    mBuildingSegmentBaseDocid =
        PartitionModifier::CalculateBuildingSegmentBaseDocId(partitionReader->GetPartitionData());
    InitBuildingSegmentModifier(inMemSegment);
}

void InplaceModifier::InitBuildingSegmentModifier(const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) {
        return;
    }
    SegmentWriterPtr segmentWriter = inMemSegment->GetSegmentWriter();
    mBuildingSegmentModifier = segmentWriter->GetInMemorySegmentModifier();
}

bool InplaceModifier::UpdateDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    docid_t docId = doc->GetDocId();
    assert(docId != INVALID_DOCID);
    if (docId < mBuildingSegmentBaseDocid) {
        // TODO(hanyao): avoid useless update
        const auto& attrDoc = doc->GetAttributeDocument();
        if (mAttrModifier and attrDoc) {
            if (!mAttrModifier->Update(docId, attrDoc)) {
                return false;
            }
        }
        const auto& indexDoc = doc->GetIndexDocument();
        if (indexDoc and mIndexModifier) {
            mIndexModifier->Update(docId, indexDoc);
        }
        return true;
    }

    docid_t localDocId = docId - mBuildingSegmentBaseDocid;
    if (mBuildingSegmentModifier) {
        return mBuildingSegmentModifier->UpdateDocument(localDocId, doc);
    }
    return false;
}

bool InplaceModifier::RemoveDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    docid_t docId = document->GetDocId();
    return RemoveDocument(docId);
}

void InplaceModifier::Dump(const DirectoryPtr& directory, segmentid_t srcSegmentId)
{
    if (mAttrModifier) {
        mAttrModifier->Dump(directory);
    }
    if (mIndexModifier) {
        mIndexModifier->Dump(directory);
    }
}

bool InplaceModifier::IsDirty() const { return false; }

bool InplaceModifier::UpdateField(docid_t docId, fieldid_t fieldId, const StringView& value, bool isNull)
{
    if (!mAttrModifier) {
        IE_LOG(WARN, "no attribute modifier");
        return false;
    }

    if (docId == INVALID_DOCID || mDeletionmapReader->IsDeleted(docId)) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mAttrModifier->UpdateField(docId, fieldId, value, isNull);
    }
    // here not support update building segment
    return false;
}

bool InplaceModifier::RedoIndex(docid_t docId, const ModifiedTokens& modifiedTokens)
{
    if (!mIndexModifier) {
        IE_LOG(ERROR, "no index modifier");
        return false;
    }
    if (docId >= mRealtimeSegmentBaseDocId) {
        // ref:   index::DynamicIndexWriter::Dump
        return true;
    }
    if (docId == INVALID_DOCID || mDeletionmapReader->IsDeleted(docId)) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mIndexModifier->UpdateField(docId, modifiedTokens);
    }
    // here not support update building segment
    return false;
}

bool InplaceModifier::UpdateIndex(index::IndexUpdateTermIterator* iterator)
{
    if (!mIndexModifier) {
        IE_LOG(ERROR, "no index modifier");
        return false;
    }
    return mIndexModifier->UpdateIndex(iterator);
}

bool InplaceModifier::UpdatePackField(docid_t docId, packattrid_t packAttrId, const StringView& value)
{
    if (!mAttrModifier) {
        IE_LOG(WARN, "no attribute modifier");
        return false;
    }

    if (docId == INVALID_DOCID || mDeletionmapReader->IsDeleted(docId)) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mAttrModifier->UpdatePackField(docId, packAttrId, value);
    }
    // here not support update building segment
    return false;
}

bool InplaceModifier::RemoveDocument(docid_t docId)
{
    if (docId == INVALID_DOCID) {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid) {
        return mDeletionmapReader->Delete(docId);
    }

    if (mBuildingSegmentModifier) {
        docid_t localDocId = docId - mBuildingSegmentBaseDocid;
        mBuildingSegmentModifier->RemoveDocument(localDocId);
        return true;
    }
    return false;
}

docid_t InplaceModifier::CalculateRealtimeSegmentBaseDocId(const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    int64_t docCount = 0;
    while (iter->IsValid()) {
        auto segmentData = iter->GetSegmentData();
        if (index_base::RealtimeSegmentDirectory::IsRtSegmentId(iter->GetSegmentId())) {
            assert(docCount == segmentData.GetBaseDocId());
            break;
        }
        docCount += segmentData.GetSegmentInfo()->docCount;
        iter->MoveToNext();
    }
    return docCount;
}

PartitionModifierDumpTaskItemPtr InplaceModifier::GetDumpTaskItem(const PartitionModifierPtr& modifier) const
{
    (void)modifier;
    PartitionModifierDumpTaskItemPtr taskItem(make_shared<PartitionModifierDumpTaskItem>());
    taskItem->taskType = PartitionModifierDumpTaskType::InplaceModifierTask;
    taskItem->buildResourceMetrics = GetBuildResourceMetrics();
    if (mAttrModifier != nullptr) {
        taskItem->packAttrUpdataBitmapItems.emplace_back(mAttrModifier->GetPackAttrUpdataBitmapVec());
    }
    return taskItem;
}

std::vector<AttributeUpdateBitmapPtr> InplaceModifier::GetPackAttrUpdataBitmapVec() const
{
    return mAttrModifier->GetPackAttrUpdataBitmapVec();
}
}} // namespace indexlib::partition
