#include "indexlib/index/normal/inverted_index/test/index_term_extender_mock.h"

#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/indexlib.h"

using namespace std;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, IndexTermExtenderMock);

IndexTermExtenderMock::IndexTermExtenderMock(const config::IndexConfigPtr& indexConfig)
    : IndexTermExtender(indexConfig, /*TruncateIndexWriterPtr=*/nullptr,
                        /*MultiAdaptiveBitmapIndexWriterPtr=*/nullptr)
{
}

IndexTermExtenderMock::~IndexTermExtenderMock() {}

std::shared_ptr<PostingIterator>
IndexTermExtenderMock::CreateCommonPostingIterator(const util::ByteSliceListPtr& sliceList, uint8_t compressMode,
                                                   SegmentTermInfo::TermIndexMode mode)
{
    BufferedPostingIterator* iter = new BufferedPostingIterator(PostingFormatOption(), NULL, nullptr);
    return std::shared_ptr<PostingIterator>(iter);
}
}}} // namespace indexlib::index::legacy
