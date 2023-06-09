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
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"

#include <unordered_set>

#include "autil/EnvUtil.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/inverted_index/OneDocMerger.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDumper.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/format/PostingFormat.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryTypedFactory.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_dedup_patch_file_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_term_extender.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_merger.h"
#include "indexlib/index/normal/util/segment_file_compress_util.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
namespace {
using indexlib::index::legacy::BitmapPostingMerger;
using indexlib::index::legacy::IndexTermExtender;
} // namespace

IE_LOG_SETUP(index, NormalIndexMerger);

NormalIndexMerger::NormalIndexMerger()
    : mPostingFormat(NULL)
    , mAllocator(NULL)
    , mByteSlicePool(NULL)
    , mBufferPool(NULL)
    , mSortMerge(false)
{
}

NormalIndexMerger::~NormalIndexMerger()
{
    // deallocate before pool
    mAdaptiveBitmapIndexWriter.reset();
    mTruncateIndexWriter.reset();
    for (IndexOutputSegmentResourcePtr& indexOutputSegmentResource : mIndexOutputSegmentResources) {
        indexOutputSegmentResource.reset();
    }
    mIndexOutputSegmentResources.clear();
    DELETE_AND_SET_NULL(mByteSlicePool);
    DELETE_AND_SET_NULL(mBufferPool);
    DELETE_AND_SET_NULL(mPostingFormat);
    DELETE_AND_SET_NULL(mAllocator);
}

void NormalIndexMerger::Init(const config::IndexConfigPtr& indexConfig, const index_base::MergeItemHint& hint,
                             const index_base::MergeTaskResourceVector& taskResources,
                             const config::MergeIOConfig& ioConfig, legacy::TruncateIndexWriterCreator* truncateCreator,
                             legacy::AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
{
    IndexMerger::Init(indexConfig, hint, taskResources, ioConfig, truncateCreator, adaptiveCreator);

    if (truncateCreator) {
        mTruncateIndexWriter = truncateCreator->Create(indexConfig, ioConfig);
        if (mTruncateIndexWriter) {
            mTruncateIndexWriter->SetIOConfig(mIOConfig);
        }
    }
    if (adaptiveCreator) {
        mAdaptiveBitmapIndexWriter = adaptiveCreator->Create(indexConfig, ioConfig);
    }
    Init(indexConfig);
}

void NormalIndexMerger::Init(const IndexConfigPtr& indexConfig)
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
    mPostingWriterResource.reset(
        new PostingWriterResource(&mSimplePool, mByteSlicePool, mBufferPool, GetPostingFormatOption()));
}

void NormalIndexMerger::Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = false;
    if (outputSegMergeInfos.size() == 0) {
        IE_LOG(ERROR, "giving MergerResource for merge is empty, cannot merge");
        return;
    }
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos);
    MergePatches(resource, segMergeInfos, outputSegMergeInfos, mIndexConfig);
}

void NormalIndexMerger::SortByWeightMerge(const index::MergerResource& resource,
                                          const index_base::SegmentMergeInfos& segMergeInfos,
                                          const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = true;
    if (outputSegMergeInfos.size() == 0) {
        IE_LOG(ERROR, "giving outputSegmentMergeInfo for merge is empty, cannot merge");
        return;
    }
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos);
    MergePatches(resource, segMergeInfos, outputSegMergeInfos, mIndexConfig);
}

void NormalIndexMerger::PrepareIndexOutputSegmentResource(
    const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    bool needCreateBitmapIndex = (mIndexConfig->GetHighFreqVocabulary() || mAdaptiveBitmapIndexWriter != nullptr);
    if (NeedPreloadMaxDictCount(outputSegMergeInfos)) {
        size_t dictKeyCount = GetDictKeyCount(segMergeInfos);
        file_system::DirectoryPtr indexDirectory = outputSegMergeInfos[0].directory;
        const DirectoryPtr& mergeDir = MakeMergeDir(indexDirectory, mIndexConfig, mIndexFormatOption, mMergeHint);
        IndexOutputSegmentResourcePtr outputResource(new IndexOutputSegmentResource(dictKeyCount));
        outputResource->Init(mergeDir, mIndexConfig, mIOConfig, outputSegMergeInfos[0].temperatureLayer, &mSimplePool,
                             needCreateBitmapIndex);
        mIndexOutputSegmentResources.push_back(outputResource);
        return;
    }

    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
        file_system::DirectoryPtr indexDirectory = outputSegMergeInfos[i].directory;
        const DirectoryPtr& mergeDir = MakeMergeDir(indexDirectory, mIndexConfig, mIndexFormatOption, mMergeHint);
        IndexOutputSegmentResourcePtr outputResource(new IndexOutputSegmentResource);
        outputResource->Init(mergeDir, mIndexConfig, mIOConfig, outputSegMergeInfos[i].temperatureLayer, &mSimplePool,
                             needCreateBitmapIndex);
        mIndexOutputSegmentResources.push_back(outputResource);
    }
}

size_t NormalIndexMerger::GetDictKeyCount(const SegmentMergeInfos& segMergeInfos) const
{
    assert(mIndexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING);
    IE_LOG(INFO, "merge index [%s], Begin GetDictKeyCount.", mIndexConfig->GetIndexName().c_str());
    std::unordered_set<dictkey_t> dictKeySet;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        segmentid_t segId = segMergeInfos[i].segmentId;
        uint32_t maxDocCount = segMergeInfos[i].segmentInfo.docCount;
        if (maxDocCount == 0) {
            continue;
        }

        IE_LOG(INFO, "read dictionary file for index [%s] in segment [%d]", mIndexConfig->GetIndexName().c_str(),
               segId);
        const index_base::PartitionDataPtr& partData = mSegmentDirectory->GetPartitionData();
        index_base::SegmentData segData = partData->GetSegmentData(segId);
        file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
            continue;
        }

        bool supportFileCompress = SegmentFileCompressUtil::IndexSupportFileCompress(mIndexConfig, segData);
        unique_ptr<DictionaryReader> dictReaderPtr(DictionaryCreator::CreateDiskReader(mIndexConfig));
        auto status = dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME, supportFileCompress);
        THROW_IF_STATUS_ERROR(status);
        std::shared_ptr<DictionaryIterator> dictIter = dictReaderPtr->CreateIterator();
        index::DictKeyInfo key;
        dictvalue_t value;
        while (dictIter->HasNext()) {
            dictIter->Next(key, value);
            if (!key.IsNull()) {
                dictKeySet.insert(key.GetKey());
            }
        }
    }

    IE_LOG(INFO, "merge index [%s], total dictKeyCount [%lu].", mIndexConfig->GetIndexName().c_str(),
           dictKeySet.size());
    return dictKeySet.size();
}

void NormalIndexMerger::InnerMerge(const index::MergerResource& resource,
                                   const index_base::SegmentMergeInfos& segMergeInfos,
                                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    assert(mMergeHint.GetId() != MergeItemHint::INVALID_PARALLEL_MERGE_ITEM_ID);
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        return;
    }

    PrepareIndexOutputSegmentResource(resource, segMergeInfos, outputSegMergeInfos);
    if (mAdaptiveBitmapIndexWriter || mTruncateIndexWriter) {
        mTermExtender.reset(new IndexTermExtender(mIndexConfig, mTruncateIndexWriter, mAdaptiveBitmapIndexWriter));
        mTermExtender->Init(outputSegMergeInfos, mIndexOutputSegmentResources);
    }

    // Init term queue
    std::shared_ptr<legacy::OnDiskIndexIteratorCreator> onDiskIndexIterCreator = CreateOnDiskIndexIteratorCreator();
    legacy::SegmentTermInfoQueue termInfoQueue(mIndexConfig, onDiskIndexIterCreator);
    termInfoQueue.Init(mSegmentDirectory, segMergeInfos);

    index::DictKeyInfo key;
    while (!termInfoQueue.Empty()) {
        SegmentTermInfo::TermIndexMode termMode;
        const SegmentTermInfos& segTermInfos = termInfoQueue.CurrentTermInfos(key, termMode);
        MergeTerm(key, segTermInfos, termMode, resource, outputSegMergeInfos);
        termInfoQueue.MoveToNextTerm();
    }

    if (mTermExtender) {
        mTermExtender->Destroy();
        mTermExtender.reset();
    }
    EndMerge();
}

void NormalIndexMerger::EndMerge()
{
    for (auto& indexOutputSegmentResource : mIndexOutputSegmentResources) {
        indexOutputSegmentResource->Reset();
    }

    mIndexOutputSegmentResources.clear();

    if (mByteSlicePool) {
        mByteSlicePool->release();
    }

    if (mBufferPool) {
        mBufferPool->release();
    }
}

int64_t NormalIndexMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                             const index_base::SegmentMergeInfos& segMergeInfos,
                                             const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                             bool isSortedMerge) const
{
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        return 0;
    }

    int64_t size = sizeof(*this);
    size += mIOConfig.GetWriteBufferSize() * 2 * outputSegMergeInfos.size(); // write buffer, dict and posting

    int64_t maxPostingLen = GetMaxLengthOfPosting(segDir, segMergeInfos);
    IE_LOG(INFO, "maxPostingLen [%ld]", maxPostingLen);
    int64_t segmentReadMemUse = maxPostingLen + mIOConfig.GetReadBufferSize() * 2; // one segment read buffer
    segmentReadMemUse += sizeof(OneDocMerger);
    if (isSortedMerge) {
        segmentReadMemUse *= segMergeInfos.size();
    }
    IE_LOG(INFO, "segmentReadMemUse [%ld]", segmentReadMemUse);
    size += segmentReadMemUse;

    int64_t mergedPostingLen = maxPostingLen * segMergeInfos.size();
    size += mergedPostingLen; // writer memory use

    if (mIndexConfig->IsHashTypedDictionary() && !NeedPreloadMaxDictCount(outputSegMergeInfos)) {
        size += GetHashDictMaxMemoryUse(segDir, segMergeInfos);
    }

    if (!mTruncateIndexWriter && !mAdaptiveBitmapIndexWriter) {
        return size;
    }

    size += mergedPostingLen; // copy merged posting to generate truncate/bitmap list

    uint32_t docCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        docCount += segMergeInfos[i].segmentInfo.docCount;
    }
    IE_LOG(INFO, "total doc count: %u", docCount);
    if (mTruncateIndexWriter) {
        int64_t truncIndexWriterMemUse =
            mTruncateIndexWriter->EstimateMemoryUse(mergedPostingLen, docCount, outputSegMergeInfos.size());
        size += truncIndexWriterMemUse;
        IE_LOG(INFO, "NormalIndexMerger EstimateMemoryUse: truncIndexWriterMemUse [%f MB]",
               (double)truncIndexWriterMemUse / 1024 / 1024);
    }
    if (mAdaptiveBitmapIndexWriter) {
        int64_t bitMapIndexWriterMemUse = mAdaptiveBitmapIndexWriter->EstimateMemoryUse(docCount);
        size += bitMapIndexWriterMemUse;
        IE_LOG(INFO, "NormalIndexMerger EstimateMemoryUse: bitMapIndexWriterMemUse [%f MB]",
               (double)bitMapIndexWriterMemUse / 1024 / 1024);
    }
    return size;
}

int64_t NormalIndexMerger::GetHashDictMaxMemoryUse(const SegmentDirectoryBasePtr& segDir,
                                                   const index_base::SegmentMergeInfos& segMergeInfos) const
{
    int64_t size = 0;
    if (segMergeInfos.size() == 1) {
        // buffer read dictionary file, to get max dict count
        return size;
    }

    const index_base::PartitionDataPtr& partData = segDir->GetPartitionData();
    for (uint32_t i = 0; i < segMergeInfos.size(); i++) {
        auto segId = segMergeInfos[i].segmentId;
        index_base::SegmentData segData = partData->GetSegmentData(segId);
        file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
            continue;
        }
        size += indexDirectory->GetFileLength(DICTIONARY_FILE_NAME);
    }
    return size;
}

int64_t NormalIndexMerger::GetMaxLengthOfPosting(const index_base::SegmentData& segData) const
{
    file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), true);

    DictionaryReader* dictReader = DictionaryCreator::CreateDiskReader(mIndexConfig);
    unique_ptr<DictionaryReader> dictReaderPtr(dictReader);
    bool supportFileCompress = SegmentFileCompressUtil::IndexSupportFileCompress(mIndexConfig, segData);
    auto status = dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME, supportFileCompress);
    THROW_IF_STATUS_ERROR(status);
    std::shared_ptr<DictionaryIterator> dictIter = dictReader->CreateIterator();

    int64_t lastOffset = 0;
    int64_t maxLen = 0;
    index::DictKeyInfo key;
    dictvalue_t value;
    while (dictIter->HasNext()) {
        dictIter->Next(key, value);
        int64_t offset = 0;
        if (!ShortListOptimizeUtil::GetOffset(value, offset)) {
            continue;
        }
        maxLen = std::max(maxLen, offset - lastOffset);
        lastOffset = offset;
    }

    size_t indexFileLength = 0;
    CompressFileInfoPtr compressInfo = indexDirectory->GetCompressFileInfo(POSTING_FILE_NAME);
    if (compressInfo) {
        indexFileLength = compressInfo->deCompressFileLen;
    } else {
        indexFileLength = indexDirectory->GetFileLength(POSTING_FILE_NAME);
    }
    maxLen = std::max(maxLen, (int64_t)indexFileLength - lastOffset);
    return maxLen;
}

int64_t NormalIndexMerger::GetMaxLengthOfPosting(const SegmentDirectoryBasePtr& segDir,
                                                 const SegmentMergeInfos& segMergeInfos) const
{
    segmentid_t segId = segMergeInfos[0].segmentId;
    uint32_t maxDocCount = segMergeInfos[0].segmentInfo.docCount;
    for (uint32_t i = 1; i < segMergeInfos.size(); i++) {
        if (segMergeInfos[i].segmentInfo.docCount > maxDocCount) {
            segId = segMergeInfos[i].segmentId;
            maxDocCount = segMergeInfos[i].segmentInfo.docCount;
        }
    }

    if (maxDocCount == 0) {
        return 0;
    }

    const index_base::PartitionDataPtr& partData = segDir->GetPartitionData();
    index_base::SegmentData segData = partData->GetSegmentData(segId);
    return GetMaxLengthOfPosting(segData);
}

void NormalIndexMerger::MergeTerm(index::DictKeyInfo key, const SegmentTermInfos& segTermInfos,
                                  SegmentTermInfo::TermIndexMode mode, const index::MergerResource& resource,
                                  const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    DoMergeTerm(key, segTermInfos, mode, resource, outputSegmentMergeInfos);
    ResetMemPool();
}

void NormalIndexMerger::DoMergeTerm(index::DictKeyInfo key, const SegmentTermInfos& segTermInfos,
                                    SegmentTermInfo::TermIndexMode mode, const index::MergerResource& resource,
                                    const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    const auto& vol = mIndexConfig->GetHighFreqVocabulary();
    bool isHighFreqTerm = vol && vol->Lookup(key);
    if (mode == SegmentTermInfo::TM_BITMAP) {
        // no high frequency term in repartition case, drop bitmap posting
        if (!isHighFreqTerm) {
            return;
        }
    } else {
        assert(mode == SegmentTermInfo::TM_NORMAL);
        if (isHighFreqTerm && mIndexConfig->GetHighFrequencyTermPostingType() != hp_both &&
            !mIndexConfig->IsTruncateTerm(key)) {
            // bitmap only term, avoid create normal posting from index patch,
            // but truncate term has a raw posting in build segment
            return;
        }
    }

    PostingMergerPtr postingMerger = MergeTermPosting(segTermInfos, mode, resource, outputSegmentMergeInfos);
    df_t df = postingMerger->GetDocFreq();
    if (df <= 0) {
        return;
    }
    IndexTermExtender::TermOperation termOperation = IndexTermExtender::TO_REMAIN;
    if (mTermExtender) {
        termOperation = mTermExtender->ExtendTerm(key, postingMerger, mode);
    }
    if (termOperation != IndexTermExtender::TO_DISCARD) {
        postingMerger->Dump(key, mIndexOutputSegmentResources);
    }
}

void NormalIndexMerger::ResetMemPool()
{
    mByteSlicePool->reset();
    mBufferPool->reset();
}

PostingMergerPtr NormalIndexMerger::MergeTermPosting(const SegmentTermInfos& segTermInfos,
                                                     SegmentTermInfo::TermIndexMode mode,
                                                     const index::MergerResource& resource,
                                                     const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    PostingMergerPtr postingMerger;
    if (mode == SegmentTermInfo::TM_BITMAP) {
        postingMerger.reset(CreateBitmapPostingMerger(outputSegmentMergeInfos));
    } else {
        postingMerger.reset(CreatePostingMerger(outputSegmentMergeInfos));
    }

    if (!mSortMerge) {
        postingMerger->Merge(segTermInfos, resource.reclaimMap);
    } else {
        postingMerger->SortByWeightMerge(segTermInfos, resource.reclaimMap);
    }
    return postingMerger;
}

void NormalIndexMerger::MergePatches(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                                     const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                     const IndexConfigPtr& config)
{
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        return;
    }
    if (!mIndexConfig->IsIndexUpdatable()) {
        return;
    }
    file_system::DirectoryPtr targetDir;
    for (const auto& outputSeg : outputSegMergeInfos) {
        if (static_cast<size_t>(outputSeg.targetSegmentIndex) + 1 == resource.targetSegmentCount) {
            targetDir = outputSeg.directory;
            break;
        }
    }
    if (!targetDir) {
        return;
    }

    IndexDedupPatchFileMerger merger(mIndexConfig);
    merger.Merge(mSegmentDirectory->GetPartitionData(), segMergeInfos,
                 targetDir->GetDirectory(mIndexConfig->GetIndexName(), true));
}

PostingMerger*
NormalIndexMerger::CreateBitmapPostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    BitmapPostingMerger* bitmapPostingMerger =
        new BitmapPostingMerger(mByteSlicePool, outputSegmentMergeInfos, mIndexConfig->GetOptionFlag());
    return bitmapPostingMerger;
}

PostingMerger*
NormalIndexMerger::CreatePostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    legacy::PostingMergerImpl* postingMergerImpl =
        new legacy::PostingMergerImpl(mPostingWriterResource.get(), outputSegmentMergeInfos);
    return postingMergerImpl;
}

bool NormalIndexMerger::NeedPreloadMaxDictCount(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) const
{
    if (outputSegMergeInfos.size() > 1) {
        // can not allocate total key count to each output
        return false;
    }

    bool preloadKeyCount = autil::EnvUtil::getEnv("INDEXLIB_PRELOAD_DICTKEY_COUNT", false);
    return preloadKeyCount && mIndexConfig->IsHashTypedDictionary();
}

DirectoryPtr NormalIndexMerger::MakeMergeDir(const DirectoryPtr& indexDirectory, config::IndexConfigPtr& indexConfig,
                                             LegacyIndexFormatOption& indexFormatOption, MergeItemHint& mergeHint)
{
    const DirectoryPtr& mergeDir = GetMergeDir(indexDirectory, true);
    string optionString = LegacyIndexFormatOption::ToString(indexFormatOption);
    mergeDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
    return mergeDir;
}
}} // namespace indexlib::index
