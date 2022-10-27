#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexRetriever);

IndexRetriever::IndexRetriever()
{
}

IndexRetriever::~IndexRetriever() 
{
}

bool IndexRetriever::Init(const DeletionMapReaderPtr& deletionMapReader,
                     const vector<IndexSegmentRetrieverPtr>& retrievers,
                     const vector<docid_t>& baseDocIds,
                     const IndexerResourcePtr& resource)
{
    if (retrievers.size() != baseDocIds.size())
    {
        IE_LOG(ERROR, "retrievers size is not equal to baseDocIds size");
        return false;
    }
    mDeletionMapReader = deletionMapReader;
    copy(retrievers.begin(), retrievers.end(), back_inserter(mSegRetrievers));
    copy(baseDocIds.begin(), baseDocIds.end(), back_inserter(mBaseDocIds));
    mIndexerResource = resource;
    return true;
}

vector<SegmentMatchInfo> IndexRetriever::Search(const Term& term, Pool* pool)
{
    vector<SegmentMatchInfo> segMatchInfos;
    segMatchInfos.reserve(mSegRetrievers.size());
    for (size_t i = 0; i < mSegRetrievers.size(); ++i)
    {
        MatchInfo matchInfo = mSegRetrievers[i]->Search(term.GetWord(), pool);
        SegmentMatchInfo segMatchInfo;
        segMatchInfo.baseDocId = mBaseDocIds[i];
        segMatchInfo.matchInfo.reset(new MatchInfo(move(matchInfo))); 
        segMatchInfos.push_back(segMatchInfo); 
    } 
    return segMatchInfos; 
}

bool IndexRetriever::SearchAsync(const common::Term &term,
                                 autil::mem_pool::Pool * pool,
                                 function<void(vector<SegmentMatchInfo>)> done) {
    vector<SegmentMatchInfo> segMatchInfos = Search(term, pool);
    done(segMatchInfos);
    return true;
}

IE_NAMESPACE_END(index);

