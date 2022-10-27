#include "indexlib/index/normal/summary/summary_writer_impl.h"
#include "indexlib/index/normal/summary/local_disk_summary_writer.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SummaryWriterImpl);

SummaryWriterImpl::SummaryWriterImpl() 
{
}

SummaryWriterImpl::~SummaryWriterImpl() 
{
}

void SummaryWriterImpl::Init(const SummarySchemaPtr& summarySchema,
                             BuildResourceMetrics* buildResourceMetrics)
{
    assert(summarySchema);
    assert(summarySchema->NeedStoreSummary());
    
    mSummarySchema = summarySchema;
    for (summarygroupid_t groupId = 0;
         groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId)
    {
        const SummaryGroupConfigPtr& groupConfig =
            mSummarySchema->GetSummaryGroupConfig(groupId);
        if (groupConfig->NeedStoreSummary())
        {
            mGroupWriterVec.push_back(
                    LocalDiskSummaryWriterPtr(new LocalDiskSummaryWriter()));
            mGroupWriterVec[groupId]->Init(groupConfig, buildResourceMetrics);
        }
        else
        {
            mGroupWriterVec.push_back(LocalDiskSummaryWriterPtr());
        }
    }
}

bool SummaryWriterImpl::AddDocument(const SerializedSummaryDocumentPtr& document)
{
    assert(document);
    if (mGroupWriterVec.size() == 1)
    {
        // only default group
        assert(document->GetDocId() == (docid_t)mGroupWriterVec[0]->GetNumDocuments());
        assert(mGroupWriterVec[0]);
        size_t len = document->GetLength();
        ConstString data(document->GetValue(), len);
        return mGroupWriterVec[0]->AddDocument(data);
    }

    // mv to SummaryFormatter::DeserializeSummaryDoc
    const char* cursor = document->GetValue();
    for (size_t i = 0; i < mGroupWriterVec.size(); ++i)
    {
        if (!mGroupWriterVec[i])
        {
            continue;
        }
        assert(document->GetDocId() == (docid_t)mGroupWriterVec[i]->GetNumDocuments());
        uint32_t len = SummaryGroupFormatter::ReadVUInt32(cursor);
        ConstString data(cursor, len);
        cursor += len;
        if (!mGroupWriterVec[i]->AddDocument(data))
        {
            return false;
        }
    }
    assert(cursor - document->GetValue() == (int64_t)document->GetLength());
    return true;
}

void SummaryWriterImpl::Dump(const DirectoryPtr& directory, PoolBase* dumpPool)
{
    if (mGroupWriterVec.empty())
    {
        return;
    }
    assert((size_t)mSummarySchema->GetSummaryGroupConfigCount() == mGroupWriterVec.size());
    assert(DEFAULT_SUMMARYGROUPID == 0);
    if (mGroupWriterVec[0])
    {
        assert(mSummarySchema->GetSummaryGroupConfig(0)->IsDefaultGroup());
        mGroupWriterVec[0]->Dump(directory, dumpPool);
    }

    for (size_t i = 1; i < mGroupWriterVec.size(); ++i)
    {
        if (!mGroupWriterVec[i])
        {
            continue;
        }
        const string& groupName = mGroupWriterVec[i]->GetGroupName();
        DirectoryPtr realDirectory = directory->MakeDirectory(groupName);
        mGroupWriterVec[i]->Dump(realDirectory, dumpPool);
    }
}

const SummarySegmentReaderPtr SummaryWriterImpl::CreateInMemSegmentReader()
{
    InMemSummarySegmentReaderContainerPtr inMemSummarySegmentReaderContainer(
            new InMemSummarySegmentReaderContainer());
    for (size_t i = 0; i < mGroupWriterVec.size(); ++i)
    {
        if (mGroupWriterVec[i])
        {
            inMemSummarySegmentReaderContainer->AddReader(
                    mGroupWriterVec[i]->CreateInMemSegmentReader());
        }
        else
        {
            inMemSummarySegmentReaderContainer->AddReader(
                    InMemSummarySegmentReaderPtr());
        }
    }
    return inMemSummarySegmentReaderContainer;
}

IE_NAMESPACE_END(index);

