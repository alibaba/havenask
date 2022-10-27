#include "indexlib/index/normal/summary/summary_reader_impl.h"
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/misc/exception.h"

using namespace std;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SummaryReaderImpl);

SummaryReaderImpl::SummaryReaderImpl(const SummarySchemaPtr& summarySchema)
    : SummaryReader(summarySchema)
{
}

SummaryReaderImpl::~SummaryReaderImpl()
{
}

bool SummaryReaderImpl::Open(const PartitionDataPtr& partitionData,
                             const PrimaryKeyIndexReaderPtr& pkIndexReader)
{
    if (!mSummarySchema)
    {
        IE_LOG(ERROR, "summary schema is empty");
        return false;
    }
    
    for (summarygroupid_t groupId = 0;
         groupId < mSummarySchema->GetSummaryGroupConfigCount(); ++groupId)
    {
        const SummaryGroupConfigPtr& summaryGroupConfig =
            mSummarySchema->GetSummaryGroupConfig(groupId);
        LocalDiskSummaryReaderPtr readerImpl(
                new LocalDiskSummaryReader(mSummarySchema, groupId));
        if (!readerImpl->Open(partitionData, pkIndexReader))
        {
            IE_LOG(ERROR, "open summary group reader[%s] failed",
                   summaryGroupConfig->GetGroupName().c_str());
            return false;
        }
        mSummaryGroups.push_back(readerImpl);
        mAllGroupIds.push_back(groupId);
    }
    return true;
}

void SummaryReaderImpl::AddAttrReader(fieldid_t fieldId,
                                      const AttributeReaderPtr& attrReader)
{
    summarygroupid_t groupId =
        mSummarySchema->FieldIdToSummaryGroupId(fieldId);
    assert(groupId >= 0 && groupId < (summarygroupid_t)mSummaryGroups.size());
    mSummaryGroups[groupId]->AddAttrReader(fieldId, attrReader);
}

void SummaryReaderImpl::AddPackAttrReader(fieldid_t fieldId,
        const PackAttributeReaderPtr& attrReader)
{
    summarygroupid_t groupId =
        mSummarySchema->FieldIdToSummaryGroupId(fieldId);
    assert(groupId >= 0 && groupId < (summarygroupid_t)mSummaryGroups.size());
    mSummaryGroups[groupId]->AddPackAttrReader(fieldId, attrReader);
}

bool SummaryReaderImpl::GetDocument(docid_t docId,
                                    SearchSummaryDocument *summaryDoc,
                                    const SummaryGroupIdVec& groupVec) const
{
    try
    {
        return DoGetDocument(docId, summaryDoc, groupVec);
    }
    catch (const misc::ExceptionBase &e)
    {
        IE_LOG(ERROR, "GetDocument exception: %s", e.what());
    }
    catch (const std::exception& e)
    {
        IE_LOG(ERROR, "GetDocument exception: %s", e.what());
    }
    catch (...)
    {
        IE_LOG(ERROR, "GetDocument exception");
    }
    return false;
}

bool SummaryReaderImpl::DoGetDocument(docid_t docId,
                                      SearchSummaryDocument *summaryDoc,
                                      const SummaryGroupIdVec& groupVec) const
{
    for (size_t i = 0; i < groupVec.size(); ++i)
    {
        if (unlikely(groupVec[i] < 0 ||
                     groupVec[i] >= (summarygroupid_t)mSummaryGroups.size()))
        {
            IE_LOG(WARN, "invalid summary group id [%d], max group id [%d]",
                   groupVec[i], (summarygroupid_t)mSummaryGroups.size());
            return false;
        }
        if (!mSummaryGroups[groupVec[i]]->GetDocument(docId, summaryDoc))
        {
            return false;
        }
    }
    return true;
}

IE_NAMESPACE_END(index);

