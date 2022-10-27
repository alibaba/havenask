#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentPosting);

void SegmentPosting::Init(const util::ByteSliceListPtr& sliceList, 
                          docid_t baseDocId, const SegmentInfo& segmentInfo, 
                          dictvalue_t dictValue)
{
    Init(ShortListOptimizeUtil::GetCompressMode(dictValue),
         sliceList, baseDocId, segmentInfo);
}

void SegmentPosting::Init(uint8_t compressMode, 
                          const util::ByteSliceListPtr& sliceList, 
                          docid_t baseDocId, const SegmentInfo& segmentInfo)
{
    mSliceListPtr = sliceList;
    mBaseDocId = baseDocId;
    mDocCount = segmentInfo.docCount;
    mCompressMode = compressMode;
    mMainChainTermMeta = GetCurrentTermMeta();
    mPostingWriter = NULL;
}

void SegmentPosting::Init(docid_t baseDocId, const SegmentInfo& segmentInfo, 
                          dictvalue_t dictValue)
{
    mBaseDocId = baseDocId;
    mDocCount = segmentInfo.docCount;
    mCompressMode = ShortListOptimizeUtil::GetCompressMode(dictValue);
    mDictInlinePostingData = ShortListOptimizeUtil::GetDictInlineValue(dictValue);
    mMainChainTermMeta = GetCurrentTermMeta();
    mPostingWriter = NULL;
}

void SegmentPosting::Init(docid_t baseDocId, uint32_t docCount,
                          PostingWriter* postingWriter)
{
    mBaseDocId = baseDocId;
    mDocCount = docCount;
    mPostingWriter = postingWriter;
    GetTermMetaForRealtime(mMainChainTermMeta);
}

TermMeta SegmentPosting::GetCurrentTermMeta() const
{
    if (ShortListOptimizeUtil::GetDocCompressMode(mCompressMode) 
        == DICT_INLINE_COMPRESS_MODE)
    {
        DictInlineFormatter formatter(
                mPostingFormatOption, mDictInlinePostingData);

        return TermMeta(formatter.GetDocFreq(),
                        formatter.GetTermFreq(), // df = 1, ttf = tf
                        formatter.GetTermPayload());
    }

    if (mPostingWriter)
    {
        //in memory segment no truncate posting list
        return mMainChainTermMeta;
    }

    TermMeta tm;
    TermMetaLoader tmLoader(mPostingFormatOption);
    ByteSliceReader reader(mSliceListPtr.get());
    tmLoader.Load(&reader, tm);
    return tm;
}

bool SegmentPosting::operator == (const SegmentPosting& segPosting) const
{
    return (mSliceListPtr == segPosting.mSliceListPtr) 
        && (mMainChainTermMeta == segPosting.mMainChainTermMeta)
        && (mPostingFormatOption == segPosting.mPostingFormatOption)
        && (mBaseDocId == segPosting.mBaseDocId)
        && (mDocCount == segPosting.mDocCount)
        && (mCompressMode == segPosting.mCompressMode)
        && (mPostingWriter == segPosting.mPostingWriter);
}

void SegmentPosting::GetTermMetaForRealtime(TermMeta& tm)
{
    assert(mPostingWriter);
    df_t df = mPostingWriter->GetDF();
    tf_t ttf = mPostingWriter->GetTotalTF();    
    termpayload_t termPayload = mPostingWriter->GetTermPayload();

    tm.SetDocFreq(df);
    tm.SetTotalTermFreq(ttf);
    tm.SetPayload(termPayload);
}

IE_NAMESPACE_END(index);

