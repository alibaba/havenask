#include <autil/ConstString.h>
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/segment/in_memory_segment_modifier.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/attribute/accessor/patch_attribute_modifier.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PatchModifier);

PatchModifier::PatchModifier(const IndexPartitionSchemaPtr& schema,
                             bool enablePackFile, bool isOffline)
    : PartitionModifier(schema)
    , mBuildingSegmentBaseDocid(0)
    , mEnablePackFile(enablePackFile)
    , mIsOffline(isOffline)
{
}

PatchModifier::~PatchModifier() 
{
}

void PatchModifier::Init(const PartitionDataPtr& partitionData,
                         const util::BuildResourceMetricsPtr& buildResourceMetrics)
{
    if (!buildResourceMetrics)
    {
        mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    }
    else
    {
        mBuildResourceMetrics = buildResourceMetrics;
    }

    mPkIndexReader = LoadPrimaryKeyIndexReader(partitionData);

    InitDeletionMapWriter(partitionData);
    InitAttributeModifier(partitionData);
    mPartitionInfo = partitionData->GetPartitionInfo();

    mBuildingSegmentBaseDocid = CalculateBuildingSegmentBaseDocId(partitionData);
    InitBuildingSegmentModifier(partitionData->GetInMemorySegment());
    
    mPartitionData = partitionData;
}

bool PatchModifier::UpdateDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!doc->HasPrimaryKey() ||
        doc->GetAttributeDocument() == NULL)
    {
        IE_LOG(WARN, "update document is invalid, IGNORE.");
        ERROR_COLLECTOR_LOG(WARN, "update document is invalid, IGNORE.");
        return false;
    }

    docid_t docId = doc->GetDocId();
    if (docId == INVALID_DOCID)
    {
        assert(mPkIndexReader);
        const string& pkString = doc->GetIndexDocument()->GetPrimaryKey();
        docId = mPkIndexReader->Lookup(pkString);
        if (docId == INVALID_DOCID)
        {
            IE_LOG(INFO, "target update document [pk:%s] is not exist!",
                   pkString.c_str());
            ERROR_COLLECTOR_LOG(ERROR, "target update document [pk:%s] is not exist!",
                    pkString.c_str());
            return false;
        }
    }

    if (docId < mBuildingSegmentBaseDocid)
    {
        if (mAttrModifier)
        {
            return mAttrModifier->Update(docId, doc->GetAttributeDocument());
        }
        IE_LOG(WARN, "no attribute modifier");
        return false;
    }

    if (mBuildingSegmentModifier)
    {
        docid_t localDocId = docId - mBuildingSegmentBaseDocid;
        return mBuildingSegmentModifier->UpdateDocument(localDocId, doc);
    }
    return false;
}

bool PatchModifier::UpdateField(docid_t docId,
                                fieldid_t fieldId, 
                                const ConstString& value)
{
    if (docId == INVALID_DOCID)
    {
        return false;
    }
    
    if (docId < mBuildingSegmentBaseDocid)
    {
        return mAttrModifier->UpdateField(docId, fieldId, value);
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
    assert(mPkIndexReader);
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!doc->HasPrimaryKey())
    {
        return false;
    }

    const string& pkString = doc->GetIndexDocument()->GetPrimaryKey();
    docid_t docId = mPkIndexReader->Lookup(pkString);
    return RemoveDocument(docId);
}

void PatchModifier::Dump(const DirectoryPtr& directory,
                         segmentid_t srcSegmentId)
{
    if (!IsDirty())
    {
        return;
    }

    if (mSchema->TTLEnabled())
    {
        UpdateInMemorySegmentInfo();
    }

    DirectoryPtr dumpDirectory = directory;
    if (mEnablePackFile)
    {
        PackDirectoryPtr packDirectory = DYNAMIC_POINTER_CAST(
                PackDirectory, directory);
        if (!packDirectory)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "dump directory is not PackDirectory!");
        }
    }

    mDeletionmapWriter->Dump(dumpDirectory);
    if (mAttrModifier)
    {
        mAttrModifier->Dump(dumpDirectory, srcSegmentId, mDumpThreadNum);
    }
}

PrimaryKeyIndexReaderPtr PatchModifier::LoadPrimaryKeyIndexReader(
        const PartitionDataPtr& partitionData)
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    assert(indexSchema);
    IndexConfigPtr pkIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig)
    {
        return PrimaryKeyIndexReaderPtr();
    }
    PrimaryKeyIndexReaderPtr pkReader(
            IndexReaderFactory::CreatePrimaryKeyIndexReader(
                    pkIndexConfig->GetIndexType()));
    assert(pkReader);
    pkReader->OpenWithoutPKAttribute(pkIndexConfig, partitionData);
    return pkReader;
}

void PatchModifier::InitDeletionMapWriter(const PartitionDataPtr& partitionData)
{
    mDeletionmapWriter.reset(new DeletionMapWriter(true));
    mDeletionmapWriter->Init(partitionData.get());
}

bool PatchModifier::IsDirty() const
{
    if (mDeletionmapWriter->IsDirty())
    {
        return true;
    }

    if (mAttrModifier)
    {
        return mAttrModifier->IsDirty();
    }
    return false;
}

bool PatchModifier::RemoveDocument(docid_t docId)
{
    if (docId == INVALID_DOCID)
    {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid)
    {
        return mDeletionmapWriter->Delete(docId);
    }

    if (mBuildingSegmentModifier)
    {
        docid_t localDocId = docId - mBuildingSegmentBaseDocid;
        mBuildingSegmentModifier->RemoveDocument(localDocId);
        return true;
    }
    return false;
}

void PatchModifier::InitAttributeModifier(
        const PartitionDataPtr& partitionData)
{
    if (!mSchema->GetAttributeSchema())
    {
        return;
    }

    mAttrModifier.reset(new PatchAttributeModifier(mSchema,
                    mBuildResourceMetrics.get(), mIsOffline));
    mAttrModifier->Init(partitionData);
}

void PatchModifier::InitBuildingSegmentModifier(
        const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment)
    {
        return; 
    }
    mSegmentWriter = inMemSegment->GetSegmentWriter();
    mBuildingSegmentModifier = mSegmentWriter->GetInMemorySegmentModifier();
}

docid_t PatchModifier::CalculateBuildingSegmentBaseDocId(
        const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);

    docid_t baseDocid = segIter->GetBuildingBaseDocId();
    SegmentIteratorPtr buildingIter = segIter->CreateIterator(SIT_BUILDING);
    while (buildingIter->IsValid())
    {
        InMemorySegmentPtr inMemSegment = buildingIter->GetInMemSegment();
        assert(inMemSegment);
        if (inMemSegment->GetStatus() != InMemorySegment::BUILDING)
        {
            baseDocid += buildingIter->GetSegmentData().GetSegmentInfo().docCount;
        }
        buildingIter->MoveToNext();
    }
    return baseDocid;
}

void PatchModifier::UpdateInMemorySegmentInfo()
{
    InMemorySegmentPtr inMemSegment = mPartitionData->GetInMemorySegment();
    if (!inMemSegment)
    {
        return;
    }

    SegmentInfoPtr segmentInfo = 
        inMemSegment->GetSegmentWriter()->GetSegmentInfo();
    if (!segmentInfo)
    {
        return;
    }

    vector<segmentid_t> patchSegIds;
    mDeletionmapWriter->GetPatchedSegmentIds(patchSegIds);
    if (mAttrModifier)
    {
        mAttrModifier->GetPatchedSegmentIds(patchSegIds);
    }
    
    sort(patchSegIds.begin(), patchSegIds.end());
    vector<segmentid_t>::iterator it =
        unique(patchSegIds.begin(), patchSegIds.end());
    patchSegIds.resize(distance(patchSegIds.begin(),it));
    for (size_t i = 0; i < patchSegIds.size(); ++i)
    {
        SegmentData segData = mPartitionData->GetSegmentData(patchSegIds[i]);
        segmentInfo->maxTTL = max(segmentInfo->maxTTL, 
                segData.GetSegmentInfo().maxTTL);
    }

    InMemorySegmentPtr subInMemSegment = inMemSegment->GetSubInMemorySegment();
    if (!subInMemSegment)
    {
        return;
    }
    SegmentInfoPtr subSegmentInfo = 
        subInMemSegment->GetSegmentWriter()->GetSegmentInfo();
    if (!subSegmentInfo)
    {
        return;
    }
    segmentInfo->maxTTL = max(subSegmentInfo->maxTTL, segmentInfo->maxTTL);
}

IE_NAMESPACE_END(partition);

