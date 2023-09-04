#include "indexlib/test/atomic_query.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, AtomicQuery);

AtomicQuery::AtomicQuery() : Query(qt_atomic) { mIter = NULL; }

AtomicQuery::~AtomicQuery()
{
    if (mIter) {
        auto pool = mIter->GetSessionPool();
        IE_POOL_COMPATIBLE_DELETE_CLASS(pool, mIter);
        mIter = NULL;
    }
}

docid_t AtomicQuery::Seek(docid_t docid) { return mIter->SeekDoc(docid); }

pos_t AtomicQuery::SeekPosition(pos_t pos)
{
    if (!mIter->HasPosition()) {
        return INVALID_POSITION;
    }
    return mIter->SeekPosition(pos);
}
}} // namespace indexlib::test
