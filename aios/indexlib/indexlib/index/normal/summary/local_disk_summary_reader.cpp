#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <assert.h>
#include <sstream>
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"
#include "indexlib/misc/exception.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"

using namespace std;
using namespace std::tr1;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, LocalDiskSummaryReader);

LocalDiskSummaryReader::LocalDiskSummaryReader(
        const SummarySchemaPtr& summarySchema, summarygroupid_t summaryGroupId)
    : SummaryReader(summarySchema)
    , mSummaryGroupConfig(summarySchema->GetSummaryGroupConfig(summaryGroupId))
    , mBuildingBaseDocId(INVALID_DOCID)
{
    assert(mSummaryGroupConfig);
}

LocalDiskSummaryReader::~LocalDiskSummaryReader() 
{
}

bool LocalDiskSummaryReader::Open(const PartitionDataPtr& partitionData,
                             const PrimaryKeyIndexReaderPtr &pkIndexReader) 
{
    IE_LOG(DEBUG, "Begin opening summary");
    mPKIndexReader = pkIndexReader;

    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    assert(builtSegIter);

    while (builtSegIter->IsValid())
    {
        const SegmentData& segData = builtSegIter->GetSegmentData();
        const SegmentInfo& segmentInfo = segData.GetSegmentInfo();
        try 
        {
            if (segmentInfo.docCount == 0)
            {
                builtSegIter->MoveToNext();
                continue;
            }
            if (!LoadSegmentReader(segData))
            {
                return false;
            }
            mSegmentInfos.push_back(segmentInfo);
            builtSegIter->MoveToNext();
        }
        catch (const ExceptionBase& e)
        {
            const file_system::DirectoryPtr& directory = segData.GetDirectory();
            IE_LOG(ERROR, "LoadSegment[%s] failed,  exception:%s", 
                   directory->GetPath().c_str(), e.what());
            throw;
        }
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(SIT_BUILDING);
    InitBuildingSummaryReader(buildingSegIter);
    IE_LOG(DEBUG, "End opening summary");
    return true;
}

bool LocalDiskSummaryReader::GetDocument(docid_t docId, 
        SearchSummaryDocument *summaryDoc) const
{
    if (GetDocumentFromSummary(docId, summaryDoc))
    {
        return GetDocumentFromAttributes(docId, summaryDoc);
    }

    return false;
}

bool LocalDiskSummaryReader::GetDocumentFromSummary(
        docid_t docId, document::SearchSummaryDocument *summaryDoc) const
{
    if (docId < 0)
    {
        return false;
    }

    if (!mSummaryGroupConfig->NeedStoreSummary())
    {
        return true;
    }

    if (docId >= mBuildingBaseDocId)
    {
        return mBuildingSummaryReader &&
            mBuildingSummaryReader->GetDocument(docId, summaryDoc);
    }
    docid_t baseDocId = 0;
    for (uint32_t i = 0; i < mSegmentInfos.size(); i++)
    {
        if (docId < baseDocId + (docid_t)mSegmentInfos[i].docCount)
        {
            return mSegmentReaders[i]->GetDocument(docId - baseDocId, summaryDoc);
        }
        baseDocId += mSegmentInfos[i].docCount;
    }
    return false;
}

bool LocalDiskSummaryReader::SetSummaryDocField(SearchSummaryDocument *summaryDoc,
                                                fieldid_t fieldId, const string& value) const
{
    assert(summaryDoc);
    int32_t summaryFieldId = mSummarySchema->GetSummaryFieldId(fieldId);
    assert(summaryFieldId != -1);
    return summaryDoc->SetFieldValue(summaryFieldId, value.data(), value.length());
}


bool LocalDiskSummaryReader::GetDocumentFromAttributes(docid_t docId,
        SearchSummaryDocument *summaryDoc) const
{
    Pool* pool = dynamic_cast<Pool*>(summaryDoc->getPool());
    string attrValue;
    for (size_t i = 0; i < mAttrReaders.size(); ++i)
    {
        if (!(mAttrReaders[i].second)->Read(docId, attrValue, pool))
        {
            return false;
        }
        if (!SetSummaryDocField(summaryDoc, mAttrReaders[i].first, attrValue))
        {
            return false;
        }
    }
    for (size_t i = 0; i < mPackAttrReaders.size(); ++i)
    {
        SummaryConfigPtr summaryConfig = mSummarySchema->
            GetSummaryConfig(mPackAttrReaders[i].first);
        assert(summaryConfig);
        const string& fieldName = summaryConfig->GetSummaryName();
        if (!(mPackAttrReaders[i].second)->Read(docId, fieldName, attrValue, pool))
        {
            return false;
        }
        if (!SetSummaryDocField(summaryDoc, mPackAttrReaders[i].first, attrValue))
        {
            return false;
        }
    }
    return true;
}

void LocalDiskSummaryReader::AddAttrReader(fieldid_t fieldId, 
        const AttributeReaderPtr& attrReader)
{
    if (attrReader)
    {
        mAttrReaders.push_back(make_pair(fieldId, attrReader));
    }
}

void LocalDiskSummaryReader::AddPackAttrReader(fieldid_t fieldId, 
        const PackAttributeReaderPtr& attrReader)
{
    if (attrReader)
    {
        mPackAttrReaders.push_back(make_pair(fieldId, attrReader));
    }
}

bool LocalDiskSummaryReader::LoadSegmentReader(
        const SegmentData& segmentData)
{
    if (!mSummaryGroupConfig->NeedStoreSummary())
    {
        mSegmentReaders.push_back(LocalDiskSummarySegmentReaderPtr());
        return true;
    }

    LocalDiskSummarySegmentReaderPtr onDiskSegReader(
            new LocalDiskSummarySegmentReader(mSummaryGroupConfig));
    
    if (!onDiskSegReader->Open(segmentData)) 
    {
        return false;
    }
    mSegmentReaders.push_back(onDiskSegReader);
    return true;
}

void LocalDiskSummaryReader::InitBuildingSummaryReader(
        const SegmentIteratorPtr& buildingIter)
{
    if (!mSummaryGroupConfig->NeedStoreSummary() || !buildingIter)
    {
        return;
    }

    while (buildingIter->IsValid())
    {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        index::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        SummarySegmentReaderPtr summarySegReader = segmentReader->GetSummaryReader();
        assert(summarySegReader);
        InMemSummarySegmentReaderContainerPtr inMemSummarySegmentReaderContainer =
            DYNAMIC_POINTER_CAST(InMemSummarySegmentReaderContainer, summarySegReader);
        assert(inMemSummarySegmentReaderContainer);

        InMemSummarySegmentReaderPtr inMemSegReader =
            inMemSummarySegmentReaderContainer->GetInMemSummarySegmentReader(
                    mSummaryGroupConfig->GetGroupId());
        if (!mBuildingSummaryReader)
        {
            mBuildingSummaryReader.reset(new BuildingSummaryReader);
        }
        mBuildingSummaryReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}

IE_NAMESPACE_END(index);
