#ifndef __INDEXLIB_INDEX_TERM_EXTENDER_MOCK_H
#define __INDEXLIB_INDEX_TERM_EXTENDER_MOCK_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/accessor/index_term_extender.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class IndexTermExtenderMock : public IndexTermExtender
{
public:
    IndexTermExtenderMock(const config::IndexConfigPtr& indexConfig);
    ~IndexTermExtenderMock();

private:
    std::shared_ptr<PostingIterator> CreateCommonPostingIterator(const util::ByteSliceListPtr& sliceList,
                                                                 uint8_t compressMode,
                                                                 SegmentTermInfo::TermIndexMode mode) override;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexTermExtenderMock);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_INDEX_TERM_EXTENDER_MOCK_H
