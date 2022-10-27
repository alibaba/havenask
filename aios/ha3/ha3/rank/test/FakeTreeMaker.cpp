#include <ha3/rank/test/FakeTreeMaker.h>
#include <ha3/rank/DistinctMap.h>
#include <matchdoc/MatchDoc.h>

using namespace std;
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, FakeTreeMaker);

FakeTreeMaker::FakeTreeMaker(common::Ha3MatchDocAllocator *allocator) {
    _allocator = allocator;
    _keyRef = _allocator->declare<int32_t>("key");
}

FakeTreeMaker::~FakeTreeMaker() {
    clearExpectTreeNodes();
}

void FakeTreeMaker::buildResultTree(const docid_t *docids, const size_t n) {
    clearExpectTreeNodes();
    _expectTreeNodes.resize(n+1);
    for (size_t j = 0; j < n; j++) {
        DistinctInfo *docItem
            = createFakeDistinctInfo(docids[j], 0);
        _expectTreeNodes[j + 1]._sdi = docItem;
    }

    for(size_t i = 1; i <= (n + 1)/ 2; i++) {
        if (2 * i <= n && docids[2*i-1] != -1) {
            HA3_LOG(TRACE1, "%zd -> %zd", i, 2 * i);
            _expectTreeNodes[i]._left = &_expectTreeNodes[2 * i];
        }
        if (2 * i + 1 <= n && docids[2*i] != -1) {
            HA3_LOG(TRACE1, "%zd -> %zd", i, 2 * i + 1);
            _expectTreeNodes[i]._right = &_expectTreeNodes[2 * i + 1];
        }
    }

//    HA3_LOG(DEBUG, "tree: %s", TreeNodeToString(&_expectTreeNodes[1]).c_str());
}

void FakeTreeMaker::clearExpectTreeNodes() {
    vector<TreeNode>::iterator it = _expectTreeNodes.begin();
    if (it == _expectTreeNodes.end()) {
        return;
    }
    for(it++; it != _expectTreeNodes.end(); it++) {
        _allocator->deallocate((matchdoc::MatchDoc)it->_sdi->getMatchDoc());
        delete it->_sdi;
    }
    _expectTreeNodes.clear();
}

const TreeNode* FakeTreeMaker::getExpectResult() {
    return &_expectTreeNodes[1];
}


DistinctInfo* FakeTreeMaker::
createFakeDistinctInfo(docid_t docid, int32_t key) {
    matchdoc::MatchDoc matchDoc = _allocator->allocate(docid);
    _keyRef->set(matchDoc, key);
    DistinctInfo *item = new DistinctInfo(matchDoc);
    return item;
}

END_HA3_NAMESPACE(rank);
