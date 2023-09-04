#include "indexlib/merger/test/demo_index_segment_retriever.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, DemoIndexSegmentRetriever);

const std::string DemoIndexSegmentRetriever::INDEX_FILE_NAME = "demo_index_file";

bool DemoIndexSegmentRetriever::DoOpen(const DirectoryPtr& indexDir)
{
    ParallelReduceMeta parallelMeta(0);
    // load index
    if (!parallelMeta.Load(indexDir)) {
        return LoadData(indexDir);
    }
    // load parallel-merged index
    assert(parallelMeta.parallelCount > 1);
    for (uint32_t i = 0; i < parallelMeta.parallelCount; i++) {
        string subDirName = ParallelMergeItem::GetParallelInstanceDirName(parallelMeta.parallelCount, i);
        DirectoryPtr subDir = indexDir->GetDirectory(subDirName, false);
        if (!LoadData(subDir)) {
            return false;
        }
    }
    return true;
}

bool DemoIndexSegmentRetriever::LoadData(const DirectoryPtr& indexDir)
{
    string fileContent;
    try {
        indexDir->Load(INDEX_FILE_NAME, fileContent);
        vector<DocItem> docItemVec;
        autil::legacy::FromJsonString(docItemVec, fileContent);
        for (const auto& docItem : docItemVec) {
            mDataMap.insert(docItem);
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "fail to load index file [%s], error: [%s]", INDEX_FILE_NAME.c_str(), e.what());
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

size_t DemoIndexSegmentRetriever::EstimateLoadMemoryUse(const DirectoryPtr& indexDir) const
{
    ParallelReduceMeta parallelMeta(0);
    // load index
    if (!parallelMeta.Load(indexDir)) {
        return indexDir->GetFileLength(DemoIndexSegmentRetriever::INDEX_FILE_NAME);
    }
    // load parallel-merged index
    assert(parallelMeta.parallelCount > 1);
    size_t totalLen = 0;
    for (uint32_t i = 0; i < parallelMeta.parallelCount; i++) {
        string subDirName = ParallelMergeItem::GetParallelInstanceDirName(parallelMeta.parallelCount, i);
        DirectoryPtr subDir = indexDir->GetDirectory(subDirName, false);
        return subDir->GetFileLength(DemoIndexSegmentRetriever::INDEX_FILE_NAME);
    }
    return totalLen;
}
}} // namespace indexlib::merger
