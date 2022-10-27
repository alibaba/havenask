#ifndef __INDEXLIB_INDEX_TERM_EXTENDER_MOCK_H
#define __INDEXLIB_INDEX_TERM_EXTENDER_MOCK_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_term_extender.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_BEGIN(index);

class IndexTermExtenderMock : public IndexTermExtender
{
public:
    IndexTermExtenderMock(const config::IndexConfigPtr& indexConfig);
    ~IndexTermExtenderMock();
public:

private:
    PostingIteratorPtr CreateCommonPostingIterator(
            const util::ByteSliceListPtr& sliceList, 
            uint8_t compressMode,
            SegmentTermInfo::TermIndexMode mode) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexTermExtenderMock);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_TERM_EXTENDER_MOCK_H
