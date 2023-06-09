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
#include "indexlib/base/Status.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingWriter.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/DocCollector.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/IEvaluator.h"
#include "indexlib/index/inverted_index/truncate/Reference.h"
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/TruncateTrigger.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {

class SingleTruncateIndexWriter : public TruncateIndexWriter
{
public:
    SingleTruncateIndexWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                              const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper);
    ~SingleTruncateIndexWriter();

public:
    void Init(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegmentMetas) override;
    bool NeedTruncate(const TruncateTriggerInfo& info) const override;
    Status AddPosting(const DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                      df_t docFreq) override;
    void EndPosting() override;
    void SetIOConfig(const file_system::IOConfig& ioConfig) override { _ioConfig = ioConfig; }

    void SetParam(const std::shared_ptr<IEvaluator>& evaluator, const std::shared_ptr<DocCollector>& collector,
                  const std::shared_ptr<TruncateTrigger>& truncTrigger, const std::string& truncateIndexName,
                  const std::shared_ptr<DocInfoAllocator>& docInfoAllocator, const file_system::IOConfig& ioConfig);

    void SetTruncateIndexMetaInfo(const std::shared_ptr<file_system::FileWriter>& metaFilePath,
                                  const std::string& firstDimenSortFieldName, bool desc);
    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const override;

private:
    std::shared_ptr<MultiSegmentPostingWriter> CreatePostingWriter(InvertedIndexType indexType);
    void AddPosition(const std::shared_ptr<PostingWriter>& positionWriter, TermMatchData& termMatchData,
                     const std::shared_ptr<PostingIterator>& postingIt);
    void EndDocument(const std::shared_ptr<PostingWriter>& positionWriter,
                     const std::shared_ptr<MultiSegmentPostingIterator>& postingIt, const TermMatchData& termMatchData,
                     docid_t docId);
    bool BuildTruncateIndex(const std::shared_ptr<PostingIterator>& postingIt,
                            const std::shared_ptr<MultiSegmentPostingWriter>& postingWriter);
    void DumpPosting(const DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                     const std::shared_ptr<MultiSegmentPostingWriter>& postingWriter);

    Status PrepareResource();
    bool HasTruncated(const DictKeyInfo& dictKey);
    void CollectDocIds(const DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIter);

    void SaveDictKey(const DictKeyInfo& dictKey);
    void WriteTruncateIndex(const DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt);
    Status WriteTruncateMeta(const DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt);
    void GenerateMetaValue(const std::shared_ptr<PostingIterator>& postingIt, const DictKeyInfo& dictKey,
                           std::string& metaValue);
    void AcquireLastDocValue(const std::shared_ptr<PostingIterator>& postingIt, std::string& value);
    void ResetResource();
    void ReleasePoolResource();
    void ReleaseMetaResource();
    void ReleaseTruncateIndexResource();

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _invertedIndexConfig;
    file_system::IOConfig _ioConfig;
    IndexFormatOption _indexFormatOption;
    std::shared_ptr<IEvaluator> _evaluator;
    std::shared_ptr<DocCollector> _collector;
    std::shared_ptr<TruncateTrigger> _truncateTrigger;
    std::shared_ptr<DocInfoAllocator> _docInfoAllocator;
    std::unordered_set<dictkey_t> _truncateDictKeySet;
    bool _hasNullTerm;

    autil::mem_pool::ChunkAllocatorBase* _allocator;
    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::RecyclePool* _bufferPool;
    util::SimplePool _simplePool;
    std::shared_ptr<PostingWriterResource> _postingWriterResource;

    std::shared_ptr<file_system::FileWriter> _metaFileWriter;
    Reference* _sortFieldRef;
    std::shared_ptr<indexlibv2::index::DocMapper> _docMapper;

    std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>> _targetSegmentMetas;
    IndexOutputSegmentResources _outputSegmentResources;
    std::string _truncateIndexName;
    bool _desc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
