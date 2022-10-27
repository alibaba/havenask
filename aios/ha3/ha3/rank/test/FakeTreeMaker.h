#ifndef ISEARCH_FAKETREEMAKER_H
#define ISEARCH_FAKETREEMAKER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/DistinctMap.h>
#include <vector>
#include <matchdoc/Reference.h>
#include <ha3/common/Ha3MatchDocAllocator.h>

BEGIN_HA3_NAMESPACE(rank);
class DistinctMap;
class FakeTreeMaker
{
public:
    FakeTreeMaker(common::Ha3MatchDocAllocator *allocator);
    ~FakeTreeMaker();
public:
    void buildResultTree(const docid_t *docids, const size_t size);

    void clearExpectTreeNodes();
    const TreeNode* getExpectResult();
private:
    DistinctInfo* createFakeDistinctInfo(docid_t docid, int32_t key);

private:
    std::vector<TreeNode> _expectTreeNodes;
    const matchdoc::Reference<int32_t> *_keyRef;
    common::Ha3MatchDocAllocator *_allocator;

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_FAKETREEMAKER_H
