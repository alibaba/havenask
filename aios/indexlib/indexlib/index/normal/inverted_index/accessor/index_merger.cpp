#include <unordered_set>
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_typed_factory.h"
#include "indexlib/index/normal/inverted_index/format/posting_format.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_dumper.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/index/normal/inverted_index/accessor/index_term_extender.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/index/normal/inverted_index/format/one_doc_merger.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/config/high_frequency_vocabulary.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexMerger);

IndexMerger::IndexMerger()
    : mPostingFormat(NULL)
    , mAllocator(NULL)
    , mByteSlicePool(NULL)
    , mBufferPool(NULL)
    , mSortMerge(false)
{
}

IndexMerger::~IndexMerger()
{
    // deallocate before pool
    mAdaptiveBitmapIndexWriter.reset();
    mTruncateIndexWriter.reset();
    for (IndexOutputSegmentResourcePtr& indexOutputSegmentResource : mIndexOutputSegmentResources)
    {
        indexOutputSegmentResource.reset();
    }
    mIndexOutputSegmentResources.clear();
    DELETE_AND_SET_NULL(mByteSlicePool);
    DELETE_AND_SET_NULL(mBufferPool);
    DELETE_AND_SET_NULL(mPostingFormat);
    DELETE_AND_SET_NULL(mAllocator);
}

void IndexMerger::Init(const config::IndexConfigPtr& indexConfig,
                       const index_base::MergeItemHint& hint,
                       const index_base::MergeTaskResourceVector& taskResources,
                       const config::MergeIOConfig& ioConfig,
                       TruncateIndexWriterCreator* truncateCreator,
                       AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
{
    mMergeHint = hint;
    mTaskResources = taskResources;
    mIOConfig = ioConfig;

    if (truncateCreator)
    {
        mTruncateIndexWriter = truncateCreator->Create(indexConfig, ioConfig);
        if (mTruncateIndexWriter)
        {
            mTruncateIndexWriter->SetIOConfig(mIOConfig);
        }
    }
    if (adaptiveCreator)
    {
        mAdaptiveBitmapIndexWriter = adaptiveCreator->Create(indexConfig, ioConfig);
    }
    Init(indexConfig);
}

void IndexMerger::Init(const IndexConfigPtr& indexConfig)
{
    mIndexConfig = indexConfig;
    mIndexFormatOption.Init(indexConfig);

    assert(!mPostingFormat);
    mPostingFormat = new PostingFormat(GetPostingFormatOption());

    assert(!mAllocator);
    mAllocator = new MMapAllocator();
    assert(mAllocator);

    assert(!mByteSlicePool);
    assert(!mBufferPool);

    mByteSlicePool = new Pool(mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    mBufferPool = new RecyclePool(mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    mPostingWriterResource.reset(new PostingWriterResource(
                    &mSimplePool, mByteSlicePool, mBufferPool, GetPostingFormatOption()));

}

void IndexMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    mSegmentDirectory = segDir;
}

void IndexMerger::Merge(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = false;
    if (outputSegMergeInfos.size() == 0)
    {
        IE_LOG(ERROR, "giving MergerResource for merge is empty, cannot merge");
        return;
    }
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos);
}

void IndexMerger::SortByWeightMerge(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = true;
    if (outputSegMergeInfos.size() == 0)
    {
        IE_LOG(ERROR, "giving outputSegmentMergeInfo for merge is empty, cannot merge");
        return;
    }
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos);
}

void IndexMerger::PrepareIndexOutputSegmentResource(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    if (NeedPreloadMaxDictCount(outputSegMergeInfos))
    {
        size_t dictKeyCount = GetDictKeyCount(segMergeInfos);
        file_system::DirectoryPtr indexDirectory = outputSegMergeInfos[0].directory;
        const DirectoryPtr& mergeDir = MakeMergeDir(
                indexDirectory, mIndexConfig, mIndexFormatOption, mMergeHint);
        IndexOutputSegmentResourcePtr outputResource(new IndexOutputSegmentResource(dictKeyCount));
        bool hasAdaptiveBitMap = mAdaptiveBitmapIndexWriter ? true : false;
        outputResource->Init(mergeDir, mIndexConfig, mIOConfig, &mSimplePool,
                             hasAdaptiveBitMap);
        mIndexOutputSegmentResources.push_back(outputResource);
        return;
    }
    
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
    {
        file_system::DirectoryPtr indexDirectory = outputSegMergeInfos[i].directory;
        const DirectoryPtr& mergeDir = MakeMergeDir(
                indexDirectory, mIndexConfig, mIndexFormatOption, mMergeHint);
        IndexOutputSegmentResourcePtr outputResource(new IndexOutputSegmentResource);
        bool hasAdaptiveBitMap = mAdaptiveBitmapIndexWriter ? true : false;
        outputResource->Init(mergeDir, mIndexConfig, mIOConfig, &mSimplePool,
                            hasAdaptiveBitMap);
        mIndexOutputSegmentResources.push_back(outputResource);
    }
}

size_t IndexMerger::GetDictKeyCount(const SegmentMergeInfos& segMergeInfos) const
{
    assert(mIndexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING);
    IE_LOG(INFO, "merge index [%s], Begin GetDictKeyCount.",
           mIndexConfig->GetIndexName().c_str());
    std::unordered_set<dictkey_t> dictKeySet;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        segmentid_t segId = segMergeInfos[i].segmentId;
        uint32_t maxDocCount = segMergeInfos[i].segmentInfo.docCount;
        if (maxDocCount == 0)
        {
            continue;
        }

        IE_LOG(INFO, "read dictionary file for index [%s] in segment [%d]",
               mIndexConfig->GetIndexName().c_str(), segId);
        const index_base::PartitionDataPtr& partData = mSegmentDirectory->GetPartitionData();
        index_base::SegmentData segData = partData->GetSegmentData(segId);
        file_system::DirectoryPtr indexDirectory = 
            segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(DICTIONARY_FILE_NAME))
        {
            continue;
        }
        unique_ptr<DictionaryReader> dictReaderPtr(
                DictionaryCreator::CreateDiskReader(mIndexConfig));
        dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME);
        DictionaryIteratorPtr dictIter = dictReaderPtr->CreateIterator();
        dictkey_t key;
        dictvalue_t value;
        while (dictIter->HasNext())
        {
            dictIter->Next(key, value);
            dictKeySet.insert(key);
        }
    }

    IE_LOG(INFO, "merge index [%s], total dictKeyCount [%lu].",
           mIndexConfig->GetIndexName().c_str(), dictKeySet.size());
    return dictKeySet.size();
}

DirectoryPtr IndexMerger::MakeMergeDir(const DirectoryPtr& indexDirectory,
                                       config::IndexConfigPtr& indexConfig,
                                       IndexFormatOption& indexFormatOption,
                                       MergeItemHint& mergeHint)
{
    const DirectoryPtr& mergeDir = GetMergeDir(indexDirectory, true);
    string optionString = IndexFormatOption::ToString(indexFormatOption);
    mergeDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
    return mergeDir;
}

DirectoryPtr IndexMerger::GetMergeDir(const DirectoryPtr& indexDirectory, bool needClear) const
{
    string mergePath = mIndexConfig->GetIndexName();
    string parallelInstDirName = mMergeHint.GetParallelInstanceDirName();
    if (!parallelInstDirName.empty())
    {
        mergePath = PathUtil::JoinPath(mergePath, parallelInstDirName);
    }
    if (needClear)
    {
        indexDirectory->RemoveDirectory(mergePath, true);
    }
    return indexDirectory->MakeDirectory(mergePath);
}

void IndexMerger::InnerMerge(const index::MergerResource& resource, 
                             const index_base::SegmentMergeInfos& segMergeInfos, 
                             const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    assert(mMergeHint.GetId() != MergeItemHint::INVALID_PARALLEL_MERGE_ITEM_ID);
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        return;
    }

    PrepareIndexOutputSegmentResource(resource, segMergeInfos, outputSegMergeInfos);
    if (mAdaptiveBitmapIndexWriter || mTruncateIndexWriter)
    {
        mTermExtender.reset(
                new IndexTermExtender(mIndexConfig, mTruncateIndexWriter, mAdaptiveBitmapIndexWriter));
        mTermExtender->Init(outputSegMergeInfos, mIndexOutputSegmentResources);
    }   

    // Init term queue
    OnDiskIndexIteratorCreatorPtr onDiskIndexIterCreator =
        CreateOnDiskIndexIteratorCreator();
    SegmentTermInfoQueue termInfoQueue(
            mIndexConfig, onDiskIndexIterCreator);
    termInfoQueue.Init(mSegmentDirectory, segMergeInfos);

    dictkey_t key = 0;
    while (!termInfoQueue.Empty())
    {
        SegmentTermInfo::TermIndexMode termMode;
        const SegmentTermInfos &segTermInfos = termInfoQueue.CurrentTermInfos(key, termMode);
        MergeTerm(key, segTermInfos, termMode, resource, outputSegMergeInfos);
        termInfoQueue.MoveToNextTerm();
    }
    if (mTermExtender)
    {
        mTermExtender->Destroy();
        mTermExtender.reset();
    }
    EndMerge();
}

void IndexMerger::EndMerge()
{
    for (auto& indexOutputSegmentResource : mIndexOutputSegmentResources)
    {
        indexOutputSegmentResource->Reset();
    }

    mIndexOutputSegmentResources.clear();

    if (mByteSlicePool)
    {
        mByteSlicePool->release();
    }

    if (mBufferPool)
    {
        mBufferPool->release();
    }
}

int64_t IndexMerger::EstimateMemoryUse(
        const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, 
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, 
        bool isSortedMerge)
{
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        return 0;
    }

    int64_t size = sizeof(*this);
    size += mIOConfig.GetWriteBufferSize() * 2 * outputSegMergeInfos.size(); // write buffer, dict and posting

    int64_t maxPostingLen = GetMaxLengthOfPosting(segDir, segMergeInfos);
    IE_LOG(INFO, "maxPostingLen [%ld]", maxPostingLen);
    int64_t segmentReadMemUse = maxPostingLen + mIOConfig.GetReadBufferSize() * 2; // one segment read buffer
    segmentReadMemUse += sizeof(index::OneDocMerger);
    if (isSortedMerge)
    {
        segmentReadMemUse *= segMergeInfos.size();
    }
    IE_LOG(INFO, "segmentReadMemUse [%ld]", segmentReadMemUse);
    size += segmentReadMemUse;

    int64_t mergedPostingLen = maxPostingLen * segMergeInfos.size();
    size += mergedPostingLen; // writer memory use

    if (mIndexConfig->IsHashTypedDictionary() &&
        !NeedPreloadMaxDictCount(outputSegMergeInfos))
    {
        size += GetHashDictMaxMemoryUse(segDir, segMergeInfos);
    }
    
    if (!mTruncateIndexWriter && !mAdaptiveBitmapIndexWriter)
    {
        return size;
    }

    size += mergedPostingLen; // copy merged posting to generate truncate/bitmap list

    uint32_t docCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        docCount += segMergeInfos[i].segmentInfo.docCount;
    }
    IE_LOG(INFO, "total doc count: %u", docCount);
    if (mTruncateIndexWriter)
    {
        int64_t truncIndexWriterMemUse = mTruncateIndexWriter->EstimateMemoryUse
                                         (mergedPostingLen, docCount, outputSegMergeInfos.size());
        size += truncIndexWriterMemUse;
        IE_LOG(INFO, "IndexMerger EstimateMemoryUse: truncIndexWriterMemUse [%f MB]",
               (double)truncIndexWriterMemUse/1024/1024);
    }
    if (mAdaptiveBitmapIndexWriter)
    {
        int64_t bitMapIndexWriterMemUse = mAdaptiveBitmapIndexWriter->EstimateMemoryUse(docCount);
        size += bitMapIndexWriterMemUse;
        IE_LOG(INFO, "IndexMerger EstimateMemoryUse: bitMapIndexWriterMemUse [%f MB]",
               (double)bitMapIndexWriterMemUse/1024/1024);
    }
    return size;
}

int64_t IndexMerger::GetHashDictMaxMemoryUse(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& segMergeInfos) const
{
    int64_t size = 0;
    if (segMergeInfos.size() == 1)
    {
        // buffer read dictionary file, to get max dict count
        return size;
    }
    
    const index_base::PartitionDataPtr& partData = segDir->GetPartitionData();    
    for (uint32_t i = 0; i < segMergeInfos.size(); i++)
    {
        auto segId = segMergeInfos[i].segmentId;
        index_base::SegmentData segData = partData->GetSegmentData(segId);
        file_system::DirectoryPtr indexDirectory = 
            segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(DICTIONARY_FILE_NAME))
        {
            continue;
        }
        size += indexDirectory->GetFileLength(DICTIONARY_FILE_NAME);
    }
    return size;
}

int64_t IndexMerger::GetMaxLengthOfPosting(
        const index_base::SegmentData& segData) const
{
    file_system::DirectoryPtr indexDirectory = 
        segData.GetIndexDirectory(mIndexConfig->GetIndexName(), true);

    DictionaryReader *dictReader = DictionaryCreator::CreateDiskReader(mIndexConfig);
    unique_ptr<DictionaryReader> dictReaderPtr(dictReader);
    dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME);
    DictionaryIteratorPtr dictIter = dictReader->CreateIterator();

    int64_t lastOffset = 0;
    int64_t maxLen = 0;
    dictkey_t key;
    dictvalue_t value;
    while (dictIter->HasNext())
    {
        dictIter->Next(key, value);
        int64_t offset = 0;
        if (!ShortListOptimizeUtil::GetOffset(value, offset))
        {
            continue;
        }
        maxLen = std::max(maxLen, offset - lastOffset);
        lastOffset = offset;
    }
    size_t indexFileLength = indexDirectory->GetFileLength(POSTING_FILE_NAME);
    maxLen = std::max(maxLen, (int64_t)indexFileLength - lastOffset);
    return maxLen;
}

int64_t IndexMerger::GetMaxLengthOfPosting(
        const SegmentDirectoryBasePtr& segDir,
        const SegmentMergeInfos& segMergeInfos) const
{
    segmentid_t segId = segMergeInfos[0].segmentId;
    uint32_t maxDocCount = segMergeInfos[0].segmentInfo.docCount;
    for (uint32_t i = 1; i < segMergeInfos.size(); i++)
    {
        if (segMergeInfos[i].segmentInfo.docCount > maxDocCount)
        {
            segId = segMergeInfos[i].segmentId;
            maxDocCount = segMergeInfos[i].segmentInfo.docCount;
        }
    }

    if (maxDocCount == 0)
    {
        return 0;
    }

    const index_base::PartitionDataPtr& partData = segDir->GetPartitionData();
    index_base::SegmentData segData = partData->GetSegmentData(segId);
    return GetMaxLengthOfPosting(segData);
}

void IndexMerger::MergeTerm(
        dictkey_t key,
        const SegmentTermInfos& segTermInfos,
        SegmentTermInfo::TermIndexMode mode,
        const index::MergerResource& resource, 
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    DoMergeTerm(key, segTermInfos, mode, resource, outputSegmentMergeInfos);
    ResetMemPool();
}

void IndexMerger::DoMergeTerm(
        dictkey_t key,
        const SegmentTermInfos& segTermInfos,
        SegmentTermInfo::TermIndexMode mode,
        const index::MergerResource& resource, 
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    if (mode == SegmentTermInfo::TM_BITMAP) {
        //no high frequency term in repartition case, drop bitmap posting
        auto vol = mIndexConfig->GetHighFreqVocabulary();
        if (!vol) {
            return;
        }
        if (!vol->Lookup(key)) {
            return;
        }
    }

    PostingMergerPtr postingMerger = MergeTermPosting(
            segTermInfos, mode, resource, outputSegmentMergeInfos);
    df_t df = postingMerger->GetDocFreq();
    if (df <= 0)
    {
        return;
    }
    IndexTermExtender::TermOperation termOperation = IndexTermExtender::TO_REMAIN;
    if (mTermExtender)
    {
        termOperation = mTermExtender->ExtendTerm(key, postingMerger, mode);
    }
    if (termOperation != IndexTermExtender::TO_DISCARD)
    {
        postingMerger->Dump(key, mIndexOutputSegmentResources);
    }
}


void IndexMerger::ResetMemPool()
{
    mByteSlicePool->reset();
    mBufferPool->reset();
}



PostingMergerPtr IndexMerger::MergeTermPosting(
        const SegmentTermInfos& segTermInfos,
        SegmentTermInfo::TermIndexMode mode,
        const index::MergerResource& resource, 
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    PostingMergerPtr postingMerger;
    if (mode == SegmentTermInfo::TM_BITMAP)
    {
        postingMerger.reset(CreateBitmapPostingMerger(outputSegmentMergeInfos));
    }
    else
    {
        postingMerger.reset(CreatePostingMerger(outputSegmentMergeInfos));
    }

    if (!mSortMerge)
    {
        postingMerger->Merge(segTermInfos, resource.reclaimMap);
    }
    else
    {
        postingMerger->SortByWeightMerge(segTermInfos, resource.reclaimMap);
    }
    return postingMerger;
}

PostingMerger* IndexMerger::CreateBitmapPostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    BitmapPostingMerger* bitmapPostingMerger = 
        new BitmapPostingMerger(mByteSlicePool, outputSegmentMergeInfos, mIndexConfig->GetOptionFlag());
    return bitmapPostingMerger;
}

PostingMerger* IndexMerger::CreatePostingMerger(
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    PostingMergerImpl* postingMergerImpl = 
        new PostingMergerImpl(mPostingWriterResource.get(), outputSegmentMergeInfos);
    return postingMergerImpl;
}

string IndexMerger::GetMergedDir(const string& rootDir) const
{
    string mergedDir = PathUtil::JoinPath(
            rootDir, mIndexConfig->GetIndexName());
    string parallelInstDirName = mMergeHint.GetParallelInstanceDirName();
    if (parallelInstDirName.empty())
    {
        return mergedDir;
    }
    return PathUtil::JoinPath(mergedDir, parallelInstDirName);
}

vector<ParallelMergeItem> IndexMerger::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir,
    const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
    bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    vector<ParallelMergeItem> emptyItems;
    return emptyItems;
}

void IndexMerger::EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
    int32_t totalParallelCount, const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "built-in index not support parallel task merge!");    
}

bool IndexMerger::NeedPreloadMaxDictCount(
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) const
{
    if (outputSegMergeInfos.size() > 1)
    {
        // can not allocate total key count to each output
        return false;
    }

    bool preloadKeyCount = EnvUtil::GetEnv("INDEXLIB_PRELOAD_DICTKEY_COUNT", false);
    return preloadKeyCount && mIndexConfig->IsHashTypedDictionary();
}


IE_NAMESPACE_END(index);
