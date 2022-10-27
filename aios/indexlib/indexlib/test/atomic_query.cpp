#include "indexlib/test/atomic_query.h"

using namespace std;

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, AtomicQuery);

AtomicQuery::AtomicQuery() 
    : Query(qt_atomic)
{
    mIter = NULL;
}

AtomicQuery::~AtomicQuery() 
{
    delete mIter;
    mIter = NULL;
}

docid_t AtomicQuery::Seek(docid_t docid)
{ 
    return mIter->SeekDoc(docid);
}

pos_t AtomicQuery::SeekPosition(pos_t pos)
{
    if (!mIter->HasPosition())
    {
        return INVALID_POSITION;
    }
    return mIter->SeekPosition(pos);
}

IE_NAMESPACE_END(test);

