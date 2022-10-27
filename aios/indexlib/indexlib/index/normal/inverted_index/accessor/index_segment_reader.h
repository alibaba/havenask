#ifndef __INDEXLIB_INDEX_SEGMENT_READER_H
#define __INDEXLIB_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, InMemBitmapIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, SegmentPosting);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);

IE_NAMESPACE_BEGIN(index);

class IndexSegmentReader
{
public:
    IndexSegmentReader() {}
    virtual ~IndexSegmentReader() {}
public:
    virtual index::AttributeSegmentReaderPtr GetSectionAttributeSegmentReader() const
    { assert(false); return index::AttributeSegmentReaderPtr(); }

    virtual index::InMemBitmapIndexSegmentReaderPtr GetBitmapSegmentReader() const
    { return index::InMemBitmapIndexSegmentReaderPtr(); }

    virtual bool GetSegmentPosting(dictkey_t key, docid_t baseDocId,
                                   index::SegmentPosting &segPosting,
                                   autil::mem_pool::Pool* sessionPool) const
    { return false; }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_SEGMENT_READER_H
