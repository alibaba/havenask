#ifndef ISEARCH_DISTINCTMAPTEST_H
#define ISEARCH_DISTINCTMAPTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ReferenceComparator.h>
#include <unittest/unittest.h>
#include <string>
#include <ha3/rank/test/FakeTreeMaker.h>
#include <ha3/rank/DistinctMap.h>
#include <vector>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/Reference.h>

BEGIN_HA3_NAMESPACE(rank);
class DistinctInfo;
class TreeNode;

class DistinctMapTest : public TESTBASE {
public:
    DistinctMapTest();
    ~DistinctMapTest();
public:
    void setUp();
    void tearDown();
public:
    static uint32_t getTreeHeight(const TreeNode *root);
    static std::string treeNodeToString(const TreeNode *root);
    static void drawTree(const TreeNode *root, int level, int idx, 
                         char *start, int32_t totalwidth);

    void fun(int n);
    DistinctInfo* doInsert(docid_t docid, uint32_t key, float score);
    void checkBalance(TreeNode *tn, const char *msg);
    DistinctInfo* createFakeDistinctInfo(docid_t docid, int32_t key);
    void deleteDistinctInfo(DistinctInfo *distInfo);
    void checkTreeNodeStatus(TreeNode *curNode, const char *msg);
private:
    DistinctMap *_dMap;
    FakeTreeMaker *_ftm;
    std::vector<DistinctInfo*> _items;

    matchdoc::Reference<int32_t> *_keyRef;
    ReferenceComparator<int32_t> *_cmp;  
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_allocator;
private:
    HA3_LOG_DECLARE();
};
#define CHECK_BALANCE(tn) checkBalance(tn, __FUNCTION__)
#define CHECK_TREE_STATUS(tn) checkTreeNodeStatus(tn, __FUNCTION__)

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_DISTINCTMAPTEST_H
