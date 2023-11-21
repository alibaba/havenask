#include "indexlib/index/normal/adaptive_bitmap/test/posting_iterator_mocker.h"

using namespace std;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, PostingIteratorMocker);

PostingIteratorMocker::PostingIteratorMocker(std::vector<docid_t>& docList)
{
    mCursor = 0;
    for (size_t i = 0; i < docList.size(); i++) {
        mDocList.push_back(docList[i]);
    }
}

PostingIteratorMocker::~PostingIteratorMocker() {}

docid64_t PostingIteratorMocker::SeekDoc(docid64_t docId)
{
    if (mCursor >= mDocList.size()) {
        return INVALID_DOCID;
    }
    return mDocList[mCursor++];
}
}} // namespace indexlib::index
