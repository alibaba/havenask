#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_segment_retriever.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DemoIndexSegmentRetriever);

const std::string DemoIndexSegmentRetriever::mFileName = "demo_index_file";

bool DemoIndexSegmentRetriever::DoOpen(const DirectoryPtr& indexDir)
{
    string fileContent;
    try {
        indexDir->Load(mFileName, fileContent);
        vector<DocItem> docItemVec;
        autil::legacy::FromJsonString(docItemVec, fileContent);
        for (const auto& docItem : docItemVec) {
            mDataMap.insert(docItem);
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "fail to load index file [%s], error: [%s]", mFileName.c_str(), e.what());
        return false;
    }
    return true;
}

MatchInfo DemoIndexSegmentRetriever::Search(const string& query, Pool* sessionPool)
{
    vector<docid_t> docIds;
    vector<matchvalue_t> matchValues;
    for (const auto& kv : mDataMap) {
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
