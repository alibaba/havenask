#ifndef __INDEXLIB_IN_MEM_BITMAP_INDEX_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_BITMAP_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/common/term.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemBitmapIndexSegmentReader : public index::IndexSegmentReader
{
public:
    typedef util::HashMap<uint64_t, BitmapPostingWriter*> PostingTable;

public:
    InMemBitmapIndexSegmentReader(const PostingTable* postingTable, 
                                  bool isNumberIndex);
    ~InMemBitmapIndexSegmentReader();

public:
    
    bool GetSegmentPosting(dictkey_t key, docid_t baseDocId,
                           SegmentPosting &segPosting,
                           autil::mem_pool::Pool* sessionPool) const override;

private:
    const PostingTable* mPostingTable; 
    bool mIsNumberIndex;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemBitmapIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_BITMAP_INDEX_SEGMENT_READER_H
