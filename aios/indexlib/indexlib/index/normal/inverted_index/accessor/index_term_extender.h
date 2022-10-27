#ifndef __INDEXLIB_INDEX_TERM_EXTENDER_H
#define __INDEXLIB_INDEX_TERM_EXTENDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/index_data_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"

IE_NAMESPACE_BEGIN(index);

class IndexTermExtender
{
public:
    enum TermOperation { TO_REMAIN, TO_DISCARD};
public:
    IndexTermExtender(
            const config::IndexConfigPtr& indexConfig,
            const TruncateIndexWriterPtr& truncateIndexWriter,
            const MultiAdaptiveBitmapIndexWriterPtr& adaptiveBitmapWriter);

    virtual ~IndexTermExtender();

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
              IndexOutputSegmentResources& indexOutputSegmentResources);

    TermOperation ExtendTerm(dictkey_t key, 
                             const PostingMergerPtr& postingMerger,
                             SegmentTermInfo::TermIndexMode mode);

    void Destroy();

private:
    bool MakeTruncateTermPosting(dictkey_t key, 
                            const PostingMergerPtr& postingMerger,
                            SegmentTermInfo::TermIndexMode mode);
    
    bool MakeAdaptiveBitmapTermPosting(dictkey_t key, 
            const PostingMergerPtr& postingMerger);


private:
    //virtual for UT
    virtual PostingIteratorPtr CreateCommonPostingIterator(
            const util::ByteSliceListPtr& sliceList, 
            uint8_t compressMode,
            SegmentTermInfo::TermIndexMode mode);

private:
    config::IndexConfigPtr mIndexConfig;
    TruncateIndexWriterPtr mTruncateIndexWriter;
    MultiAdaptiveBitmapIndexWriterPtr mAdaptiveBitmapIndexWriter;
    index::IndexFormatOption mIndexFormatOption;
    friend class IndexTermExtenderTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexTermExtender);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_TERM_EXTENDER_H
