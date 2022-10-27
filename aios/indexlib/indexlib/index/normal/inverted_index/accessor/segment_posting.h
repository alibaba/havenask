#ifndef __INDEXLIB_SEGMENT_POSTING_H
#define __INDEXLIB_SEGMENT_POSTING_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

class SegmentPosting
{
public:
    inline SegmentPosting(const index::PostingFormatOption& postingFormatOption = OPTION_FLAG_ALL)
        : mBaseDocId(INVALID_DOCID)
        , mDocCount(0)
        , mCompressMode(0)
        , mMainChainTermMeta()
        , mPostingWriter(NULL)
        , mDictInlinePostingData(INVALID_DICT_VALUE)
        , mPostingFormatOption(postingFormatOption)
    {}

    ~SegmentPosting() {}

public:
    void Init(const util::ByteSliceListPtr& sliceList, 
              docid_t baseDocId, const index_base::SegmentInfo& segmentInfo,
              dictvalue_t dictValue);

    void Init(uint8_t compressMode, 
              const util::ByteSliceListPtr& sliceList, 
              docid_t baseDocId, const index_base::SegmentInfo& segmentInfo);

    //for dict inline compress  
    void Init(docid_t baseDocId, const index_base::SegmentInfo& segmentInfo, 
              dictvalue_t dictValue);

    // for realtime segment posting
    void Init(docid_t baseDocId, uint32_t docCount,
              PostingWriter* postingWriter);

    const util::ByteSliceListPtr& GetSliceListPtr() const
    { return mSliceListPtr; }

    index::TermMeta GetCurrentTermMeta() const;

    void SetMainChainTermMeta(const index::TermMeta& termMeta)
    { mMainChainTermMeta = termMeta; }
    const index::TermMeta& GetMainChainTermMeta() const
    { return mMainChainTermMeta; }

    docid_t GetBaseDocId() const { return mBaseDocId; }
    void SetBaseDocId(docid_t baseDocId) { mBaseDocId = baseDocId; }

    uint32_t GetDocCount() const { return mDocCount; }
    void SetDocCount(const uint32_t docCount) { mDocCount = docCount; }

    uint8_t GetCompressMode() const { return mCompressMode; }
    int64_t GetDictInlinePostingData() const { return mDictInlinePostingData; }

    const PostingWriter* GetInMemPostingWriter() const
    { return mPostingWriter; }

    bool IsRealTimeSegment() const { return mPostingWriter != NULL; }
    bool operator == (const SegmentPosting& segPosting) const;

    const index::PostingFormatOption& GetPostingFormatOption() const
    { return mPostingFormatOption; }

    void SetPostingFormatOption(const index::PostingFormatOption& option)
    { mPostingFormatOption = option; }

    bool IsDictInline() const
    { 
        return ShortListOptimizeUtil::GetDocCompressMode(
                mCompressMode) == DICT_INLINE_COMPRESS_MODE;
    }

    void Reset()
    {
        mBaseDocId = INVALID_DOCID;
        mDocCount = 0;
        mCompressMode = 0;
        mMainChainTermMeta.Reset();
        mPostingWriter = NULL;
        mDictInlinePostingData = INVALID_DICT_VALUE;
    }

private:
    void GetTermMetaForRealtime(index::TermMeta& tm);

private:
    util::ByteSliceListPtr mSliceListPtr;
    docid_t mBaseDocId;
    uint32_t mDocCount;
    uint8_t mCompressMode;
    index::TermMeta mMainChainTermMeta; 
    PostingWriter* mPostingWriter;

    int64_t mDictInlinePostingData;
    index::PostingFormatOption mPostingFormatOption;
    
private:
    friend class BufferedPostingIteratorTest;
    friend class SegmentPostingTest;
    IE_LOG_DECLARE();
};

typedef std::vector<SegmentPosting> SegmentPostingVector;
typedef std::shared_ptr<SegmentPostingVector> SegmentPostingVectorPtr;

DEFINE_SHARED_PTR(SegmentPosting);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_POSTING_H
