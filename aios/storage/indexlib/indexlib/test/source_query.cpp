#include "indexlib/test/source_query.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, SourceQuery);

SourceQuery::SourceQuery() : Query(qt_source), mIter(nullptr), mGroupId(-1) {}

SourceQuery::~SourceQuery()
{
    if (mIter) {
        auto pool = mIter->GetSessionPool();
        IE_POOL_COMPATIBLE_DELETE_CLASS(pool, mIter);
        mIter = NULL;
    }
}

bool SourceQuery::Init(index::PostingIterator* iter, std::string indexName, index::sourcegroupid_t groupId)
{
    mIter = iter;
    mIndexName = indexName;
    mGroupId = groupId;
    return true;
}

index::sourcegroupid_t SourceQuery::GetGroupId() const { return mGroupId; }

docid_t SourceQuery::Seek(docid_t docid) { return mIter->SeekDoc(docid); }

pos_t SourceQuery::SeekPosition(pos_t pos)
{
    if (!mIter->HasPosition()) {
        return INVALID_POSITION;
    }
    return mIter->SeekPosition(pos);
}

string SourceQuery::GetIndexName() const { return mIndexName; }
}} // namespace indexlib::test
