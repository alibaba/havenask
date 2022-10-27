#include <ha3/worker/FixClangSymbol.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index_base/partition_data.h>
#include <indexlib/index/normal/reclaim_map/reclaim_map.h>

using namespace std;

BEGIN_HA3_NAMESPACE(worker);

using namespace heavenask::indexlib;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);

HA3_LOG_SETUP(worker, FixClangSymbol);

class FakeIndexReduceItem : public IndexReduceItem {
public:
    FakeIndexReduceItem() {}
    virtual ~FakeIndexReduceItem() {}
public:
    virtual bool LoadIndex(const DirectoryPtr& indexDir) {
        return true;
    }
    virtual bool UpdateDocId(const DocIdMap& docIdMap) {
        return true;
    }
};

FixClangSymbol::FixClangSymbol() {
}

FixClangSymbol::~FixClangSymbol() {
}

IndexReduceItem *FixClangSymbol::createIndexReduceItem() {
    return new FakeIndexReduceItem();
}

void FixClangSymbol::notUse() {
    // IndexRetriever
    IndexRetriever indexRetriever;
    DeletionMapReaderPtr deletionMapReaderPtr;
    std::vector<IndexSegmentRetrieverPtr> retrievers;
    std::vector<docid_t> baseDocIds;
    bool res = indexRetriever.Init(deletionMapReaderPtr, retrievers, baseDocIds);
    HA3_LOG(DEBUG, "%d", (int)res);
    // IndexReduceItem
    IndexReduceItem *indexReduceItemPtr = createIndexReduceItem();
    DirectoryPtr indexDir;
    docid_t baseDocId = 0;
    index::ReclaimMapPtr reclaimMap;
    DocIdMap docIdMap(baseDocId, reclaimMap);
    res = indexReduceItemPtr->LoadIndex(indexDir);
    HA3_LOG(DEBUG, "%d", (int)res);
    res = indexReduceItemPtr->UpdateDocId(docIdMap);
    HA3_LOG(DEBUG, "%d", (int)res);
}

END_HA3_NAMESPACE(worker);
