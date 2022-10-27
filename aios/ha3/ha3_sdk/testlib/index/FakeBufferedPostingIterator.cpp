#include <ha3_sdk/testlib/index/FakeBufferedPostingIterator.h>

IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(index, FakeBufferedPostingIterator);

FakeBufferedPostingIterator::FakeBufferedPostingIterator(
        const PostingFormatOption& postingFormatOption,
        autil::mem_pool::Pool *sessionPool)
    : BufferedPostingIterator(postingFormatOption, sessionPool)
{ 
}

IE_NAMESPACE_END(index);

