#include "indexlib/index/normal/inverted_index/test/index_term_extender_mock.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/indexlib.h"

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexTermExtenderMock);

IndexTermExtenderMock::IndexTermExtenderMock(
        const config::IndexConfigPtr& indexConfig) 
    : IndexTermExtender(indexConfig,
                        TruncateIndexWriterPtr(),
                        MultiAdaptiveBitmapIndexWriterPtr())
{
}

IndexTermExtenderMock::~IndexTermExtenderMock() 
{
}

PostingIteratorPtr IndexTermExtenderMock::CreateCommonPostingIterator(
        const util::ByteSliceListPtr& sliceList, 
        uint8_t compressMode,
        SegmentTermInfo::TermIndexMode mode)
{
    BufferedPostingIterator *iter = new BufferedPostingIterator(PostingFormatOption(), NULL);
    return PostingIteratorPtr(iter);
}


IE_NAMESPACE_END(index);

