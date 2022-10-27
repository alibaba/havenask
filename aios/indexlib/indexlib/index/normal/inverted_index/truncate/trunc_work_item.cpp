#include "indexlib/index/normal/inverted_index/truncate/trunc_work_item.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncWorkItem);

TruncWorkItem::TruncWorkItem(dictkey_t dictKey, 
                             const PostingIteratorPtr& postingIt,
                             const TruncateIndexWriterPtr& indexWriter)
{
    mDictKey = dictKey;
    mPostingIt = postingIt;
    mIndexWriter = indexWriter;
}

TruncWorkItem::~TruncWorkItem() 
{
}

void TruncWorkItem::process() 
{
    assert(mIndexWriter);
    assert(mPostingIt);
    mIndexWriter->AddPosting(mDictKey, mPostingIt);
}

void TruncWorkItem::destroy()
{
    delete this;
}

void TruncWorkItem::drop()
{
    destroy();
}

IE_NAMESPACE_END(index);

