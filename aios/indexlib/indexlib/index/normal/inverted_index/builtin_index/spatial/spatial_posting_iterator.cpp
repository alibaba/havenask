
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_posting_iterator.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SpatialPostingIterator);

SpatialPostingIterator::SpatialPostingIterator(
        autil::mem_pool::Pool* sessionPool) 
    : mCurDocid(INVALID_DOCID)
    , mTermMeta(NULL)
    , mSessionPool(sessionPool)
    , mSeekDocCounter(0)
{
}

SpatialPostingIterator::~SpatialPostingIterator() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mTermMeta);
    for (size_t i = 0; i < mPostingIterators.size(); i++)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mPostingIterators[i]);
    }
}

void SpatialPostingIterator::Init(const std::vector<BufferedPostingIterator*>& postingIterators)
{
    df_t docFreq = 0;
    tf_t totalTF = 0;
    for (size_t i = 0; i < postingIterators.size(); i++)
    {
        mPostingIterators.push_back(postingIterators[i]);
        TermMeta* termMeta = postingIterators[i]->GetTermMeta();
        docFreq += termMeta->GetDocFreq();
        totalTF += termMeta->GetTotalTermFreq();
        docid_t docid = postingIterators[i]->SeekDoc(INVALID_DOCID);
        if (docid != INVALID_DOCID)
        {
            mHeap.push(make_pair(postingIterators[i], docid));
        }
    }
    mTermMeta = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
            index::TermMeta, docFreq, totalTF, 0);
}

void SpatialPostingIterator::Unpack(TermMatchData& termMatchData)
{
    //do nothing because spatial index not support docpayload, fieldmap, tf
    termMatchData.SetMatched(true);
}

void SpatialPostingIterator::Reset()
{
    mCurDocid = INVALID_DOCID;
    while(!mHeap.empty())
    {
        mHeap.pop();
    }

    for (size_t i = 0; i < mPostingIterators.size(); i++)
    {
        auto iter = mPostingIterators[i];        
        iter->Reset();
        docid_t docid = iter->SeekDoc(INVALID_DOCID);
        if (docid != INVALID_DOCID)
        {
            mHeap.push(make_pair(iter, docid));
        }
    }
}

PostingIterator* SpatialPostingIterator::Clone() const
{
    SpatialPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            SpatialPostingIterator, mSessionPool);
    assert(iter != NULL);
    vector<BufferedPostingIterator*> newPostingIterators;
    for (size_t i = 0; i < mPostingIterators.size(); i++)
    {
        PostingIterator* subPostingIterator = mPostingIterators[i]->Clone();
        newPostingIterators.push_back((BufferedPostingIterator*)subPostingIterator);
    }
    iter->Init(newPostingIterators);
    return iter;
}

IE_NAMESPACE_END(index);

