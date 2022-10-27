#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/truncate/single_truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_typed_factory.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/util/mmap_allocator.h"
#include "indexlib/storage/file_system_wrapper.h"


using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SingleTruncateIndexWriter);

struct DictionaryRecordComparator
{
    bool operator()(const SingleTruncateIndexWriter::DictionaryRecord& left,
                    const SingleTruncateIndexWriter::DictionaryRecord& right)
    {
        return left.first < right.first;
    }
};

SingleTruncateIndexWriter::SingleTruncateIndexWriter(
        const config::IndexConfigPtr& indexConfig,
        const ReclaimMapPtr& reclaimMap)
    : mIndexConfig(indexConfig)
    , mAllocator(NULL)
    , mByteSlicePool(NULL)
    , mBufferPool(NULL)
    , mSortFieldRef(NULL)
    , mReclaimMap(reclaimMap)
{
    mIndexFormatOption.Init(indexConfig);
}

SingleTruncateIndexWriter::~SingleTruncateIndexWriter() 
{
    mMetaFileWriter.reset();
    // mFileWriter.reset();
    ReleasePoolResource();
}

void SingleTruncateIndexWriter::Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    mIndexOutputSegmentMergeInfos = outputSegmentMergeInfos;
}

void SingleTruncateIndexWriter::Init(
        const EvaluatorPtr& evaluator,
        const DocCollectorPtr& collector,
        const TruncateTriggerPtr& truncTrigger,
        const string& truncateIndexName,
        const DocInfoAllocatorPtr& docInfoAllocator,
        const config::MergeIOConfig& ioConfig)
{
    mEvaluator = evaluator;
    mCollector = collector;
    mTruncTrigger = truncTrigger;
    mDocInfoAllocator = docInfoAllocator;
    mIOConfig = ioConfig;
    mTruncateIndexName = truncateIndexName;
    mAllocator = new MMapAllocator();
    mByteSlicePool = new Pool(mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    // TODO, if in memory segment use truncate, align size should be 8
    mBufferPool = new RecyclePool(mAllocator, 
                                  DEFAULT_CHUNK_SIZE * 1024 * 1024);
    mPostingWriterResource.reset(new PostingWriterResource(&mSimplePool,
                    mByteSlicePool, mBufferPool,
                    mIndexFormatOption.GetPostingFormatOption()));
    
    mTruncateDictKeySet.clear();

}

void SingleTruncateIndexWriter::SetTruncateIndexMetaInfo(
        const FileWrapperPtr& metaFile,
        const string &firstDimenSortFieldName)
{
    if (!metaFile)
    {
        return;
    }
    mMetaFileWriter = metaFile;
    // mMetaFileWriter.reset(new file_system::BufferedFileWriter());
    // mMetaFileWriter->Open(metaFilePath);

    mSortFieldRef = mDocInfoAllocator->GetReference(firstDimenSortFieldName);
    assert(mSortFieldRef != NULL);
}

void SingleTruncateIndexWriter::AddPosting(dictkey_t dictKey, 
        const PostingIteratorPtr& postingIt, df_t docFreq)
{
    PrepareResource();
    if (HasTruncated(dictKey))
    {
        return;
    }

    mCollector->CollectDocIds(dictKey, postingIt, docFreq);
    if (!mCollector->Empty())
    {
        WriteTruncateMeta(dictKey, postingIt);
        WriteTruncateIndex(dictKey, postingIt);
    }

    ResetResource();
}

bool SingleTruncateIndexWriter::BuildTruncateIndex(
        const PostingIteratorPtr& postingIt,
        const MultiSegmentPostingWriterPtr& postingWriter)
{
    const DocCollector::DocIdVectorPtr &docIdVec = mCollector->GetTruncateDocIds();
    for (size_t i = 0; i < docIdVec->size(); ++i)
    {
        docid_t docId = (*docIdVec)[i];
        docid_t seekedDocId = postingIt->SeekDoc(docId);
        assert(seekedDocId == docId); (void) seekedDocId;
        pair<segmentindex_t, docid_t> docidInfo = mReclaimMap->GetLocalId(seekedDocId);
        auto postingWriterImpl = postingWriter->GetSegmentPostingWriter(docidInfo.first);

        TruncatePostingIteratorPtr truncateIter
            = DYNAMIC_POINTER_CAST(TruncatePostingIterator, postingIt);
        MultiSegmentPostingIteratorPtr multiIter;
        if (truncateIter)
        {
            multiIter = DYNAMIC_POINTER_CAST(
                MultiSegmentPostingIterator, truncateIter->GetCurrentIterator());
        }
        assert(multiIter);
        TermMatchData termMatchData;
        multiIter->Unpack(termMatchData);
        AddPosition(postingWriterImpl, termMatchData, multiIter);
        EndDocument(postingWriterImpl, multiIter, termMatchData, docidInfo.second);
    }
    postingWriter->EndSegment();
    return true;
}

void SingleTruncateIndexWriter::AddPosition(const PostingWriterPtr& postingWriter, 
        TermMatchData& termMatchData, const PostingIteratorPtr& postingIt)
{
    InDocPositionState *inDocPosState = termMatchData.GetInDocPositionState();
    if (inDocPosState == NULL) {
        return;
    }
    InDocPositionIterator *posIter = inDocPosState->CreateInDocIterator();
    if (postingIt->HasPosition())
    {
        pos_t pos = 0;
        while ((pos = posIter->SeekPosition(pos)) != INVALID_POSITION)
        {
            postingWriter->AddPosition(pos, posIter->GetPosPayload(), 0);
        }
    }
    else if (mIndexConfig->GetOptionFlag() & of_position_list)
    {
        // add fake pos (0) for bitmap truncate
        postingWriter->AddPosition(0, posIter->GetPosPayload(), 0);
    }
    delete posIter;
    termMatchData.FreeInDocPositionState();
}

void SingleTruncateIndexWriter::EndDocument(
        const PostingWriterPtr& postingWriter, 
        const MultiSegmentPostingIteratorPtr& postingIt, 
        const TermMatchData& termMatchData, docid_t docId)
{
    assert(mIndexConfig);
    if (mIndexConfig->GetOptionFlag() & of_term_payload)
    {
        postingWriter->SetTermPayload(postingIt->GetTermPayLoad());
    }
    if (termMatchData.HasFieldMap())
    {
        fieldmap_t fieldMap = termMatchData.GetFieldMap();
        postingWriter->EndDocument(docId, postingIt->GetDocPayload(), fieldMap);
    }
    else
    {
        postingWriter->EndDocument(docId, postingIt->GetDocPayload());
    }
}

void SingleTruncateIndexWriter::DumpPosting(
        dictkey_t dictKey,
        const PostingIteratorPtr& postingIt,
        const MultiSegmentPostingWriterPtr& postingWriter)
{
    TruncatePostingIteratorPtr truncateIter
        = DYNAMIC_POINTER_CAST(TruncatePostingIterator, postingIt);
    MultiSegmentPostingIteratorPtr multiIter;
    if (truncateIter)
    {
         multiIter = DYNAMIC_POINTER_CAST(
                 MultiSegmentPostingIterator, truncateIter->GetCurrentIterator());
    }
    assert(multiIter);
    postingWriter->Dump(dictKey, mOutputSegmentResources, multiIter->GetTermPayLoad());
}

void SingleTruncateIndexWriter::SaveDictKey(
        dictkey_t dictKey, int64_t fileOffset)
{
    mTruncateDictKeySet.insert(dictKey);
}

MultiSegmentPostingWriterPtr SingleTruncateIndexWriter::CreatePostingWriter(
        IndexType indexType)
{
    MultiSegmentPostingWriterPtr writer;
    switch (indexType)
    {
    case it_primarykey64:
    case it_primarykey128:
    case it_trie:
        break;
    default:
    PostingFormatOption formatOption =
        mIndexFormatOption.GetPostingFormatOption();
        writer.reset(
            new MultiSegmentPostingWriter(
                    mPostingWriterResource.get(), mIndexOutputSegmentMergeInfos,
                    formatOption));
        break;
    }
    return writer;
}

void SingleTruncateIndexWriter::EndPosting()
{
    ReleaseMetaResource();
    ReleaseTruncateIndexResource();
    ReleasePoolResource();
}

bool SingleTruncateIndexWriter::NeedTruncate(const TruncateTriggerInfo &info) const
{
    return mTruncTrigger->NeedTruncate(info);
}

void SingleTruncateIndexWriter::PrepareResource()
{
    if (mOutputSegmentResources.size() > 0)
    {
        return;
    }
    for (size_t i = 0; i < mIndexOutputSegmentMergeInfos.size(); ++i)
    {
        const file_system::DirectoryPtr& indexDir = mIndexOutputSegmentMergeInfos[i].directory;
        indexDir->RemoveDirectory(mTruncateIndexName, true);
        const file_system::DirectoryPtr& truncateIndexDir = indexDir->MakeDirectory(mTruncateIndexName);
        IndexOutputSegmentResourcePtr indexOutputSegmentResource(new IndexOutputSegmentResource);
        indexOutputSegmentResource->Init(
            truncateIndexDir, mIndexConfig, mIOConfig, &mSimplePool, false);
        mOutputSegmentResources.push_back(indexOutputSegmentResource);
        string optionString = IndexFormatOption::ToString(mIndexFormatOption);
        truncateIndexDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
    }
}

bool SingleTruncateIndexWriter::HasTruncated(dictkey_t dictKey)
{
    if (mTruncateDictKeySet.find(dictKey) != mTruncateDictKeySet.end())
    {
        // term is already truncated
        return true;
    }
    return false;
}

void SingleTruncateIndexWriter::WriteTruncateIndex(
        dictkey_t dictKey, 
        const PostingIteratorPtr& postingIt)
{
    MultiSegmentPostingWriterPtr postingWriter = CreatePostingWriter(
            mIndexConfig->GetIndexType());
    postingIt->Reset();

    if (BuildTruncateIndex(postingIt, postingWriter))
    {
        // int64_t offset = mFileWriter->GetLength();
        int64_t offset = 0;
        DumpPosting(dictKey, postingIt, postingWriter);
        SaveDictKey(dictKey, offset);
    }

    IE_LOG(DEBUG, "index writer allocator use bytes : %luM", 
           mAllocator->getUsedBytes() / (1024 * 1024));
    postingWriter.reset();
}

void SingleTruncateIndexWriter::WriteTruncateMeta(
        dictkey_t dictKey, 
        const PostingIteratorPtr& postingIt)
{
    if (!mMetaFileWriter)
    {
        return;
    }

    string metaValue;
    GenerateMetaValue(postingIt, dictKey, metaValue);
    if (!metaValue.empty())
    {
        mMetaFileWriter->Write(metaValue.c_str(), metaValue.size());
    }
}

void SingleTruncateIndexWriter::GenerateMetaValue(
        const PostingIteratorPtr& postingIt, 
        dictkey_t dictKey, string& metaValue)
{
    string value;
    AcquireLastDocValue(postingIt, value);
    string key = StringUtil::toString(dictKey);
    metaValue = key + "\t" + value + "\n";
}

void SingleTruncateIndexWriter::AcquireLastDocValue(
        const PostingIteratorPtr& postingIt, string& value)
{
    docid_t docId = mCollector->GetMinValueDocId();
    // assert(docId != INVALID_DOCID);
    DocInfo *docInfo = mDocInfoAllocator->Allocate();
    docInfo->SetDocId(docId);
    mEvaluator->Evaluate(docId, postingIt, docInfo);
    value = mSortFieldRef->GetStringValue(docInfo);
}

void SingleTruncateIndexWriter::ResetResource()
{
    mBufferPool->reset();
    mByteSlicePool->reset();
    mCollector->Reset();
}

void SingleTruncateIndexWriter::ReleasePoolResource()
{
    if (mByteSlicePool)
    {
        mByteSlicePool->release();
        delete mByteSlicePool;
        mByteSlicePool = NULL;
    }
    if (mBufferPool)
    {
        mBufferPool->release();
        delete mBufferPool;
        mBufferPool = NULL;
    }
    if (mAllocator)
    {
        delete mAllocator;
        mAllocator = NULL;
    }
}

void SingleTruncateIndexWriter::ReleaseMetaResource()
{ 
    if (mDocInfoAllocator)
    {
        mDocInfoAllocator->Release();
    }
    if (mMetaFileWriter)
    {
        mMetaFileWriter->Close();
        mMetaFileWriter.reset();
    }
}

void SingleTruncateIndexWriter::ReleaseTruncateIndexResource()
{
    for (auto& indexOutputSegmentResource : mOutputSegmentResources)
    {
        indexOutputSegmentResource->Reset();
    }

    mOutputSegmentResources.clear();

    mTruncateDictKeySet.clear();

    if (mCollector)
    {
        mCollector.reset();
        IE_LOG(DEBUG, "clear collector");
    }
}

int64_t SingleTruncateIndexWriter::EstimateMemoryUse(
        int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const
{
    int64_t size = sizeof(*this);
    size += maxPostingLen; // for build truncate index
    int64_t writeBufferSize = mIOConfig.GetWriteBufferSize() * 2 * outputSegmentCount; // dict and data
    IE_LOG(INFO, "SingleTruncateIndexWriter: write buffer [%ld MB]",
           writeBufferSize/1024/1024);
    size += writeBufferSize;

    int64_t collectorMemUse = mCollector->EstimateMemoryUse(totalDocCount);
    IE_LOG(INFO, "SingleTruncateIndexWriter: collectorMemUse [%ld MB]",
           collectorMemUse/1024/1024);
    size += collectorMemUse;

    return size;
}

IE_NAMESPACE_END(index);

