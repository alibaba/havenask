#ifndef __INDEXLIB_SINGLE_TRUNCATE_INDEX_WRITER_H
#define __INDEXLIB_SINGLE_TRUNCATE_INDEX_WRITER_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_collector.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_trigger.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/storage/io_config.h"
#include "indexlib/util/simple_pool.h"
#include <autil/mem_pool/ChunkAllocatorBase.h>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <tr1/memory>
#include <tr1/unordered_set>
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class SingleTruncateIndexWriter : public TruncateIndexWriter
{
public:
    typedef std::tr1::unordered_set<dictkey_t> PostingSet;
    typedef std::pair<dictkey_t, dictvalue_t> DictionaryRecord;
    typedef std::vector<DictionaryRecord> DictionaryRecords;

public:
    SingleTruncateIndexWriter(const config::IndexConfigPtr& indexConfig, 
                              const ReclaimMapPtr& reclaimMap);
    virtual ~SingleTruncateIndexWriter();

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) override;
    bool NeedTruncate(const TruncateTriggerInfo &info) const override;
    void AddPosting(dictkey_t dictKey, 
                    const PostingIteratorPtr& postingIt,
                    df_t docFreq = -1) override;
    void EndPosting() override;
    void SetIOConfig(const config::MergeIOConfig &ioConfig) override 
    { 
        mIOConfig = ioConfig; 
    }

    void Init(const EvaluatorPtr& evaluator, 
              const DocCollectorPtr& collector,
              const TruncateTriggerPtr& truncTrigger,
              const std::string& truncateIndexName,
              const DocInfoAllocatorPtr& docInfoAllocator, 
              const config::MergeIOConfig& ioConfig);

    void SetTruncateIndexMetaInfo(const storage::FileWrapperPtr &metaFilePath, 
                                  const std::string &firstDimenSortFieldName);

    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                              size_t outputSegmentCount) const override;

protected:
    virtual MultiSegmentPostingWriterPtr CreatePostingWriter(IndexType indexType);
    virtual void AddPosition(const PostingWriterPtr& positionWriter, 
                             TermMatchData& termMatchData, 
                             const PostingIteratorPtr& postingIt);
    virtual void EndDocument(const PostingWriterPtr& positionWriter, 
                             const MultiSegmentPostingIteratorPtr& postingIt, 
                             const TermMatchData& termMatchData, docid_t docId);
    bool BuildTruncateIndex(
        const PostingIteratorPtr& postingIt, 
        const MultiSegmentPostingWriterPtr& postingWriter);
    void DumpPosting(dictkey_t dictKey, 
                     const PostingIteratorPtr& postingIt,
                     const MultiSegmentPostingWriterPtr& postingWriter);

    void PrepareResource();
    bool HasTruncated(dictkey_t dictKey);
    void CollectDocIds(dictkey_t key, const PostingIteratorPtr& postingIter);

    void SaveDictKey(dictkey_t dictKey, int64_t fileOffset);
    void WriteTruncateIndex(dictkey_t dictKey, const PostingIteratorPtr& postingIt);
    void WriteTruncateMeta(dictkey_t dictKey, 
                           const PostingIteratorPtr& postingIt);
    void GenerateMetaValue(const PostingIteratorPtr& postingIt, 
                           dictkey_t dictKey, std::string& metaValue);
    virtual void AcquireLastDocValue(const PostingIteratorPtr& postingIt, std::string& value);
    void ResetResource();
    void ReleasePoolResource();
    void ReleaseMetaResource();
    void ReleaseTruncateIndexResource();
//TODO delete?
    void DumpDictionary();

protected:
    config::IndexConfigPtr mIndexConfig;
    config::MergeIOConfig mIOConfig;
    index::IndexFormatOption mIndexFormatOption;
    EvaluatorPtr mEvaluator;
    DocCollectorPtr mCollector;
    TruncateTriggerPtr mTruncTrigger;
    DocInfoAllocatorPtr mDocInfoAllocator;
    // file_system::BufferedFileWriterPtr mFileWriter;
    PostingSet  mTruncateDictKeySet;

    autil::mem_pool::ChunkAllocatorBase* mAllocator;
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    PostingWriterResourcePtr mPostingWriterResource;

    //file_system::BufferedFileWriterPtr mMetaFileWriter;
    storage::FileWrapperPtr mMetaFileWriter;
    Reference* mSortFieldRef;
    index_base::OutputSegmentMergeInfos mIndexOutputSegmentMergeInfos;
    ReclaimMapPtr mReclaimMap;

    IndexOutputSegmentResources mOutputSegmentResources;
    std::string mTruncateIndexName;
private:
    friend class SingleTruncateIndexWriterTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleTruncateIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_TRUNCATE_INDEX_WRITER_H
