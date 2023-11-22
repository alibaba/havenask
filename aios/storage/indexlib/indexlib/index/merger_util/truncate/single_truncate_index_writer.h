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
#pragma once

#include <memory>
#include <unordered_set>

#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/merger_util/truncate/doc_collector.h"
#include "indexlib/index/merger_util/truncate/evaluator.h"
#include "indexlib/index/merger_util/truncate/reference.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_trigger.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index::legacy {

class SingleTruncateIndexWriter : public TruncateIndexWriter
{
public:
    typedef std::unordered_set<dictkey_t> PostingSet;

public:
    SingleTruncateIndexWriter(const config::IndexConfigPtr& indexConfig, const ReclaimMapPtr& reclaimMap);
    virtual ~SingleTruncateIndexWriter();

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) override;
    bool NeedTruncate(const TruncateTriggerInfo& info) const override;
    void AddPosting(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                    df_t docFreq = -1) override;
    void EndPosting() override;
    void SetIOConfig(const config::MergeIOConfig& ioConfig) override { mIOConfig = ioConfig; }

    void Init(const EvaluatorPtr& evaluator, const DocCollectorPtr& collector, const TruncateTriggerPtr& truncTrigger,
              const std::string& truncateIndexName, const DocInfoAllocatorPtr& docInfoAllocator,
              const config::MergeIOConfig& ioConfig);

    void SetTruncateIndexMetaInfo(const file_system::FileWriterPtr& metaFilePath,
                                  const std::string& firstDimenSortFieldName, bool desc);
    void SetReTruncateInfo(EvaluatorPtr reTruncateEvaluator, std::unique_ptr<DocCollector> reTruncateCollector,
                           bool desc);

    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const override;

protected:
    virtual std::shared_ptr<legacy::MultiSegmentPostingWriter> CreatePostingWriter(InvertedIndexType indexType);
    virtual void AddPosition(const std::shared_ptr<PostingWriter>& positionWriter, TermMatchData& termMatchData,
                             const std::shared_ptr<PostingIterator>& postingIt);
    virtual void EndDocument(const std::shared_ptr<PostingWriter>& positionWriter,
                             const std::shared_ptr<MultiSegmentPostingIterator>& postingIt,
                             const TermMatchData& termMatchData, docid_t docId);
    bool BuildTruncateIndex(const DocCollector& collector, const std::shared_ptr<PostingIterator>& postingIt,
                            const std::shared_ptr<legacy::MultiSegmentPostingWriter>& postingWriter);
    void DumpPosting(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                     const std::shared_ptr<legacy::MultiSegmentPostingWriter>& postingWriter);

    void PrepareResource();
    bool HasTruncated(const index::DictKeyInfo& dictKey);
    void CollectDocIds(const index::DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIter);

    void SaveDictKey(const index::DictKeyInfo& dictKey, int64_t fileOffset);
    void WriteTruncateIndex(const DocCollector& collector, const index::DictKeyInfo& dictKey,
                            const std::shared_ptr<PostingIterator>& postingIt);
    std::string WriteTruncateMeta(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt);
    void GenerateMetaValue(const std::shared_ptr<PostingIterator>& postingIt, const index::DictKeyInfo& dictKey,
                           std::string& metaValue);
    virtual void AcquireLastDocValue(const std::shared_ptr<PostingIterator>& postingIt, std::string& value);
    void ResetResource();
    void ReleasePoolResource();
    void ReleaseMetaResource();
    void ReleaseTruncateIndexResource();
    // TODO delete?
    void DumpDictionary();

protected:
    config::IndexConfigPtr mIndexConfig;
    config::MergeIOConfig mIOConfig;
    LegacyIndexFormatOption mIndexFormatOption;
    EvaluatorPtr mEvaluator;
    DocCollectorPtr mCollector;
    EvaluatorPtr mReTruncateEvaluator;
    std::unique_ptr<DocCollector> mReTruncateCollector;
    TruncateTriggerPtr mTruncTrigger;
    DocInfoAllocatorPtr mDocInfoAllocator;
    // file_system::BufferedFileWriterPtr mFileWriter;
    PostingSet mTruncateDictKeySet;
    bool mHasNullTerm;

    autil::mem_pool::ChunkAllocatorBase* mAllocator;
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    std::shared_ptr<PostingWriterResource> mPostingWriterResource;

    file_system::FileWriterPtr mMetaFileWriter;
    Reference* mSortFieldRef;
    index_base::OutputSegmentMergeInfos mIndexOutputSegmentMergeInfos;
    ReclaimMapPtr mReclaimMap;

    IndexOutputSegmentResources mOutputSegmentResources;
    std::string mTruncateIndexName;
    bool mDesc;

private:
    friend class SingleTruncateIndexWriterTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleTruncateIndexWriter);
} // namespace indexlib::index::legacy
