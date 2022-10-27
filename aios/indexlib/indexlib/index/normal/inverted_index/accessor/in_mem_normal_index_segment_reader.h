#ifndef __INDEXLIB_IN_MEM_NORMAL_INDEX_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_NORMAL_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/util/hash_map.h"

DECLARE_REFERENCE_CLASS(index, PostingWriter);

IE_NAMESPACE_BEGIN(index);

class InMemNormalIndexSegmentReader : public IndexSegmentReader
{
public:
    typedef util::HashMap<dictkey_t, index::PostingWriter*> PostingTable;
public:
    InMemNormalIndexSegmentReader(
            const PostingTable *postingTable,
            const index::AttributeSegmentReaderPtr& sectionSegmentReader,
            const index::InMemBitmapIndexSegmentReaderPtr& bitmapSegmentReader,
            const index::IndexFormatOption& indexFormatOption);
    ~InMemNormalIndexSegmentReader();
public:
    
    index::AttributeSegmentReaderPtr GetSectionAttributeSegmentReader() const override
    { return mSectionSegmentReader; }

    
    index::InMemBitmapIndexSegmentReaderPtr GetBitmapSegmentReader() const override
    { return mBitmapSegmentReader; }

    
    bool GetSegmentPosting(dictkey_t key, docid_t baseDocId,
                           index::SegmentPosting &segPosting,
                           autil::mem_pool::Pool* sessionPool) const override;

private:
    const PostingTable* mPostingTable;
    index::AttributeSegmentReaderPtr mSectionSegmentReader;
    index::InMemBitmapIndexSegmentReaderPtr mBitmapSegmentReader;
    index::IndexFormatOption mIndexFormatOption;
    
private:
    friend class InMemNormalIndexSegmentReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemNormalIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_NORMAL_INDEX_SEGMENT_READER_H
