#include "indexlib/index/normal/inverted_index/customized_index/test/demo_in_mem_segment_retriever.h"

#include "autil/mem_pool/Pool.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DemoInMemSegmentRetriever);

MatchInfo DemoInMemSegmentRetriever::Search(const string& query, Pool* sessionPool)
{
    assert(mDataMap);
    vector<docid_t> docIds;
    vector<matchvalue_t> matchValues;
    for (const auto& kv : (*mDataMap)) {
        if (kv.second.find(query) != string::npos) {
            docIds.push_back(kv.first);
            matchvalue_t value;
            value.SetInt32(kv.second.size());
            matchValues.push_back(value);
        }
    }

    MatchInfo matchInfo;
    matchInfo.pool = sessionPool;
    matchInfo.matchCount = docIds.size();
    matchInfo.docIds = (docid_t*)IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, docid_t, docIds.size());
    matchInfo.matchValues = (matchvalue_t*)IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, matchvalue_t, matchValues.size());
    std::copy(docIds.begin(), docIds.end(), matchInfo.docIds);
    std::copy(matchValues.begin(), matchValues.end(), matchInfo.matchValues);
    return matchInfo;
}
}} // namespace indexlib::index
