#include "indexlib/test/or_query.h"

using namespace std;

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, OrQuery);

OrQuery::OrQuery() 
    : Query(qt_or)
{
    mInited = false;
    mCursor = 0;
}

OrQuery::~OrQuery() 
{
}

docid_t OrQuery::Seek(docid_t docid)
{
    if (!mInited)
    {
        Init();
        mInited = true;
    }
    
    if (mCursor < mDocIds.size())
    {
        return mDocIds[mCursor++];
    }
    return INVALID_DOCID;
}

void OrQuery::Init()
{
    set<docid_t> docIds;
    for (size_t i = 0; i < mQuerys.size(); ++i)
    {
        docid_t docId = INVALID_DOCID;
        do
        {
            docId = mQuerys[i]->Seek(docId);
            if (docId != INVALID_DOCID)
            {
                docIds.insert(docId);
            }
        } 
        while (docId != INVALID_DOCID);
    }
    set<docid_t>::iterator it = docIds.begin();
    for (; it != docIds.end(); ++it)
    {
        mDocIds.push_back(*it);
    }
}

pos_t OrQuery::SeekPosition(pos_t pos)
{ 
    return INVALID_POSITION; 
}

IE_NAMESPACE_END(test);

