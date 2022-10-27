#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InplaceModifier);

InplaceModifier::InplaceModifier(const IndexPartitionSchemaPtr& schema)
    : PartitionModifier(schema)
    , mBuildingSegmentBaseDocid(0)
{
}

InplaceModifier::~InplaceModifier() 
{
}

void InplaceModifier::Init(const IndexPartitionReaderPtr& partitionReader,
                           const InMemorySegmentPtr& inMemSegment,
                           const util::BuildResourceMetricsPtr& buildResourceMetrics)
{
    mPkIndexReader = partitionReader->GetPrimaryKeyReader();
    mDeletionmapReader = partitionReader->GetDeletionMapReader();
    if (!buildResourceMetrics)
    {
        mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
        mBuildResourceMetrics->Init();
    }
    else
    {
        mBuildResourceMetrics = buildResourceMetrics;
    }
    
    if (mSchema->GetAttributeSchema())
    {
        mAttrModifier.reset(new InplaceAttributeModifier(mSchema,
                        mBuildResourceMetrics.get()));
        mAttrModifier->Init(partitionReader->GetAttributeReaderContainer(),
                            partitionReader->GetPartitionData());
    }

    mPartitionInfo = partitionReader->GetPartitionInfo();
    mBuildingSegmentBaseDocid = CalculateBuildingSegmentBaseDocId(
            partitionReader->GetPartitionData());
    InitBuildingSegmentModifier(inMemSegment);
}

void InplaceModifier::InitBuildingSegmentModifier(
        const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment)
    {
        return; 
    }
    SegmentWriterPtr segmentWriter = inMemSegment->GetSegmentWriter();
    mBuildingSegmentModifier = segmentWriter->GetInMemorySegmentModifier();
}

bool InplaceModifier::UpdateDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!doc->HasPrimaryKey() ||
        doc->GetAttributeDocument() == NULL)
    {
        IE_LOG(WARN, "update document is invalid, IGNORE.");
        return false;
    }

    if (!mAttrModifier)
    {
        IE_LOG(WARN, "no attribute modifier.");
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
            IE_LOG(TRACE1, "target update document [pk:%s] is not exist!",
                   pkString.c_str());
            return false;
        }
    }
    doc->SetSegmentIdBeforeModified(mPartitionInfo->GetSegmentId(docId));

    if (docId < mBuildingSegmentBaseDocid)
    {
        return mAttrModifier->Update(docId, doc->GetAttributeDocument());
    }

    docid_t localDocId = docId - mBuildingSegmentBaseDocid;
    if (mBuildingSegmentModifier)
    {
        return mBuildingSegmentModifier->UpdateDocument(
                localDocId, doc);
    }
    return false;
}

bool InplaceModifier::RemoveDocument(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!doc->HasPrimaryKey())
    {
        return false;
    }

    assert(mPkIndexReader);
    const string& pkString = doc->GetIndexDocument()->GetPrimaryKey();
    docid_t docId = mPkIndexReader->Lookup(pkString);
    doc->SetSegmentIdBeforeModified(mPartitionInfo->GetSegmentId(docId));
    return RemoveDocument(docId);
}

void InplaceModifier::Dump(
        const DirectoryPtr& directory, segmentid_t srcSegmentId)
{
    if (mAttrModifier)
    {
        mAttrModifier->Dump(directory);        
    }
}

bool InplaceModifier::IsDirty() const
{
    return false;
}

bool InplaceModifier::UpdateField(docid_t docId, fieldid_t fieldId, 
                                  const ConstString& value)
{
    if (!mAttrModifier)
    {
        IE_LOG(WARN, "no attribute modifier");
        return false;        
    }

    if (docId == INVALID_DOCID || mDeletionmapReader->IsDeleted(docId))
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

bool InplaceModifier::UpdatePackField(docid_t docId, packattrid_t packAttrId, 
                                       const ConstString& value)
{
    if (!mAttrModifier)
    {
        IE_LOG(WARN, "no attribute modifier");
        return false;        
    }

    if (docId == INVALID_DOCID || mDeletionmapReader->IsDeleted(docId))
    {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid)
    {
        return mAttrModifier->UpdatePackField(docId, packAttrId, value);            
    }
    // here not support update building segment
    return false;
}
     
bool InplaceModifier::RemoveDocument(docid_t docId)
{
    if (docId == INVALID_DOCID)
    {
        return false;
    }

    if (docId < mBuildingSegmentBaseDocid)
    {
        return mDeletionmapReader->Delete(docId);
    }
    
    if (mBuildingSegmentModifier)
    {
        docid_t localDocId = docId - mBuildingSegmentBaseDocid;
        mBuildingSegmentModifier->RemoveDocument(localDocId);
        return true;
    }
    return false;
}

docid_t InplaceModifier::CalculateBuildingSegmentBaseDocId(
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

IE_NAMESPACE_END(partition);

