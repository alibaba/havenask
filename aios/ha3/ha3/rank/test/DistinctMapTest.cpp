#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ReferenceComparator.h>
#include <string>
#include <ha3/rank/test/FakeTreeMaker.h>
#include <ha3/rank/DistinctMap.h>
#include <vector>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/DistinctMap.h>
#include <memory>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <ha3/rank/test/FakeTreeMaker.h>
#include <ha3/rank/test/DistinctMapTest.h>
#include <matchdoc/MatchDoc.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(rank);

HA3_LOG_SETUP(rank, DistinctMapTest);

DistinctMapTest::DistinctMapTest() { 
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool);
}

DistinctMapTest::~DistinctMapTest() { 
    delete _allocator;
    delete _pool;
}

void DistinctMapTest::setUp() { 
    _ftm = new FakeTreeMaker(_allocator);
    _keyRef = _allocator->declare<int32_t>("key");
    _cmp = new ReferenceComparator<int32_t>(_keyRef);
    _dMap = new DistinctMap(_cmp, 1024);
}

void DistinctMapTest::tearDown() { 
    delete _ftm;
    delete _dMap;

    if (_cmp) {
        delete _cmp;
    }

    for (vector<DistinctInfo*>::iterator it = _items.begin(); 
         it != _items.end(); it++) 
    {
        deleteDistinctInfo(*it);
    }
    _items.clear();    

}

TEST_F(DistinctMapTest, testIsEmpty) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_dMap->isEmpty());
}

TEST_F(DistinctMapTest, testInsertToEmptyMap) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {101};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    DistinctInfo *docItem 
        = createFakeDistinctInfo(101, 1);

    _items.push_back(docItem);
    ASSERT_TRUE(docItem);

    _dMap->insert(docItem);
    ASSERT_TRUE(!_dMap->isEmpty());
    ASSERT_TRUE(docItem->getTreeNode());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()),
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(docItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testLeftLeftRotate) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 103};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    DistinctInfo *rootItem = NULL;
    (void)doInsert(103, 3, 1.0);
    rootItem = doInsert(102, 2, 1.0);
    (void)doInsert(101, 1, 1.0);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testLeftLeftRotate2) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {103, 102, 105, 101, -1, 104, 107};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    DistinctInfo *rootItem = NULL;

    DistinctInfo *sdi5 = doInsert(105, 5, 1.0);
    rootItem = doInsert(103, 3, 1.0);
    (void)doInsert(107, 7, 1.0);
    (void)doInsert(102, 2, 1.0);
    DistinctInfo *sdi4 = doInsert(104, 4, 1.0);
    (void)doInsert(101, 1, 1.0);

    ASSERT_EQ(sdi5->getTreeNode(), sdi4->getTreeNode()->_parent);
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testMultiLevelLeftLeftRotate) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {103, 102, 105, 101, -1, 104, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    (void)doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    DistinctInfo *rootItem = doInsert(103, 3, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(104, 4, 1.0);
    (void)doInsert(101, 1, 1.0);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testRightRightRotate) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 103};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    (void)doInsert(101, 1, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    DistinctInfo *rootItem = doInsert(102, 2, 1.0);
    (void)doInsert(103, 3, 1.0);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()),
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()),
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testRightRightRotate2) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {104, 102, 105, 101, 103, -1, 107};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    DistinctInfo *rootItem = NULL;

    DistinctInfo *sdi2 = doInsert(102, 2, 1.0);
    (void)doInsert(101, 1, 1.0);
    rootItem = doInsert(104, 4, 1.0);
    DistinctInfo *sdi3 = doInsert(103, 3, 1.0);
    (void)doInsert(105, 5, 1.0);
    (void)doInsert(107, 7, 1.0);

    ASSERT_EQ(sdi2->getTreeNode(), sdi3->getTreeNode()->_parent);
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}


TEST_F( DistinctMapTest, testLeftRightRotate) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 103};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    (void)doInsert(103, 3, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(101, 1, 1.0);
    DistinctInfo *rootItem = doInsert(102, 2, 1.0);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()),
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()),
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testLeftRightRotate2) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {104, 102, 105, 101, 103, -1, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    (void)doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(101, 1, 1.0);
    DistinctInfo *rootItem = doInsert(104, 4, 1.0);
    (void)doInsert(103, 3, 1.0);


    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testLeftRightRotate3) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {103, 102, 105, 101, -1, 104, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    (void)doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(101, 1, 1.0);
    DistinctInfo *rootItem = doInsert(103, 3, 1.0);
    (void)doInsert(104, 4, 1.0);


    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testRightLeftRotate) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 103};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    (void)doInsert(101, 1, 1.0);
    (void)doInsert(103, 3, 1.0);
    DistinctInfo *rootItem = doInsert(102, 2, 1.0);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testRightLeftRotate2) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {103, 102, 105, 101, -1, 104, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    (void)doInsert(102, 2, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(101, 1, 1.0);
    (void)doInsert(105, 5, 1.0);
    DistinctInfo* rootItem = doInsert(103, 3, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(104, 4, 1.0);


    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testRightLeftRotate3) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {104, 102, 105, 101, 103, -1, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    (void)doInsert(102, 2, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(101, 1, 1.0);
    (void)doInsert(105, 5, 1.0);
    DistinctInfo* rootItem = doInsert(104, 4, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(103, 3, 1.0);


    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F( DistinctMapTest, testDuplicateInsert) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    DistinctInfo *rootItem = doInsert(101, 1, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 1, 1.0);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(rootItem->getTreeNode()));
}

TEST_F(DistinctMapTest, testSingleItemRemove) {
    HA3_LOG(DEBUG, "Begin Test!");
    DistinctInfo *docItem 
        = createFakeDistinctInfo(1, 1);

    _items.push_back(docItem);
    ASSERT_TRUE(docItem);
    _dMap->insert(docItem);
    ASSERT_TRUE(!_dMap->isEmpty());
    ASSERT_TRUE(_dMap->remove(docItem));
    ASSERT_TRUE(_dMap->isEmpty());
}

TEST_F(DistinctMapTest, testRemoveNodeReplacedByDirectRightChild) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {103, 101, 104};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    DistinctInfo *docItem2 = doInsert(102, 2, 1.0);
    (void)doInsert(101, 1, 1.0);
    (void)doInsert(103, 3, 1.0);
    (void)doInsert(104, 4, 1.0);

    _dMap->remove(docItem2);
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
}

TEST_F(DistinctMapTest, testRemoveNodeReplacedByDirectLeftChild) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 104};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    DistinctInfo *docItem3 = doInsert(103, 3, 1.0);
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(104, 4, 1.0);
    (void)doInsert(101, 1, 1.0);

    _dMap->remove(docItem3);
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
}

TEST_F(DistinctMapTest, testSingleItemRemoveRightMidNodeBalanceIsZero) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {107, 105, 108,-1, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    DistinctInfo *docItem5 = doInsert(105, 5, 1.0);
    DistinctInfo *docItem1 = doInsert(101, 1, 1.0);
    DistinctInfo *docItem7 = doInsert(107, 7, 1.0);
    (void)doInsert(106, 6, 1.0);
    DistinctInfo *docItem8 = doInsert(108, 8, 1.0);

    _dMap->remove(docItem1);
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
    ASSERT_EQ(-1,(int32_t)(docItem7->getTreeNode()->_balance));
    ASSERT_EQ(0, (int32_t)(docItem8->getTreeNode()->_balance));
    ASSERT_EQ(1, (int32_t)(docItem5->getTreeNode()->_balance));
}

TEST_F(DistinctMapTest, testSingleItemRemoveLeftMidNodeBalanceIsZero) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {105, 101, 107, -1, -1, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    DistinctInfo *docItem7 = doInsert(107, 7, 1.0);
    DistinctInfo *docItem5 = doInsert(105, 5, 1.0);
    DistinctInfo *docItem8 = doInsert(108, 8, 1.0);
    DistinctInfo *docItem1 = doInsert(101, 1, 1.0);
    (void)doInsert(106, 6, 1.0);


    _dMap->remove(docItem8);

    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
    ASSERT_EQ(1,(int32_t)(docItem5->getTreeNode()->_balance));
    ASSERT_EQ(0, (int32_t)(docItem1->getTreeNode()->_balance));
    ASSERT_EQ(-1, (int32_t)(docItem7->getTreeNode()->_balance));
}

TEST_F(DistinctMapTest, testLeafNodeRemoveBalance) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {104, 102, 105, -1, 103, -1, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    
    (void)doInsert(104, 4, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(105, 5, 1.0);
    DistinctInfo *docItem1 = doInsert(101, 1, 1.0);
    DistinctInfo *docItem3 = doInsert(103, 3, 1.0);
    DistinctInfo *docItem6 = doInsert(106, 6, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem1));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    docid_t result2[] = {104, 102, 105, -1, -1, -1, 106};
    _ftm->buildResultTree(result2, sizeof(result2)/sizeof(result2[0]));

    ASSERT_TRUE(_dMap->remove(docItem3));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());

    docid_t result3[] = {104, 102, 105, -1, -1, -1, -1};
    _ftm->buildResultTree(result3, sizeof(result3)/sizeof(result3[0]));

    ASSERT_TRUE(_dMap->remove(docItem6));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

TEST_F(DistinctMapTest, testLeafNodeRemoveUnBalance) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 103};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    
    (void)doInsert(103, 3, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 2, 1.0);
    DistinctInfo *docItem4 = doInsert(104, 4, 1.0);
    (void)doInsert(101, 1, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem4));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

TEST_F(DistinctMapTest, testSingleChildNodeRemoveBalance) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {102, 101, 104};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    (void)doInsert(102, 2, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(101, 1, 1.0);
    DistinctInfo *docItem3 = doInsert(103, 3, 1.0);
    (void)doInsert(104, 4, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem3));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

TEST_F(DistinctMapTest, testSingleChildNodeRemoveUnBalance) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {103, 102, 105, 101, -1, 104, 106};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    (void)doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(103, 3, 1.0);
    DistinctInfo *docItem7 = doInsert(107, 7, 1.0);
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(104, 4, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(101, 1, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem7));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

TEST_F(DistinctMapTest, testDoubleChildNodeRemoveBalance) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {104, 102, 107, 101, 103, 106, 108};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    DistinctInfo *docItem5 = doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(107, 7, 1.0);
    (void)doInsert(101, 1, 1.0);
    (void)doInsert(104, 4, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(108, 8, 1.0);
    (void)doInsert(103, 3, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem5));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

TEST_F(DistinctMapTest, testDoubleChildNodeRemoveUnBalance) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {104, 102, 106, 101, 103, -1, 107};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));

    
    DistinctInfo *docItem5 = doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(103, 3, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(104, 4, 1.0);
    (void)doInsert(107, 7, 1.0);
    (void)doInsert(101, 1, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem5));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

TEST_F(DistinctMapTest, testRemoveDoubleRotate) {
    HA3_LOG(DEBUG, "Begin Test!");
    docid_t result[] = {108, 105, 110, 103, 106, 109, 112, 
                        102, 104, -1, 107, -1, -1, 111};
    _ftm->buildResultTree(result, sizeof(result)/sizeof(result[0]));
    
    (void)doInsert(105, 5, 1.0);
    ASSERT_TRUE(!_dMap->isEmpty());
    (void)doInsert(102, 2, 1.0);
    (void)doInsert(108, 8, 1.0);
    DistinctInfo *docItem1 = doInsert(101, 1, 1.0);
    (void)doInsert(103, 3, 1.0);
    (void)doInsert(106, 6, 1.0);
    (void)doInsert(110, 10, 1.0);
    (void)doInsert(104, 4, 1.0);
    (void)doInsert(107, 7, 1.0);
    (void)doInsert(109, 9, 1.0);
    (void)doInsert(112, 12, 1.0);
    (void)doInsert(111, 11, 1.0);

    ASSERT_TRUE(_dMap->remove(docItem1));
    ASSERT_EQ(treeNodeToString(_ftm->getExpectResult()), 
                         treeNodeToString(_dMap->getRoot()));
    CHECK_BALANCE(_dMap->getRoot());
    CHECK_TREE_STATUS(_dMap->getRoot());
}

uint32_t DistinctMapTest::getTreeHeight(const TreeNode *root) {
    uint32_t leftHeight = 0;
    uint32_t rightHeight = 0;
    if (!root) {
        return 0;
    }

    leftHeight = getTreeHeight(root->_left);
    rightHeight = getTreeHeight(root->_right);

    return (leftHeight > rightHeight) ? leftHeight + 1 : rightHeight + 1;
}

string DistinctMapTest::treeNodeToString(const TreeNode *root) {
    if (!root) {
        return "";
    }
    stringstream ss;
    uint32_t height = getTreeHeight(root);
    int32_t outputWidth = 5 * (1 << (height - 1));
    int32_t outputHeight = 1 + 3 * (height - 1);
    int32_t totalLength = outputWidth * outputHeight;
    char *output = (char*)malloc(totalLength + 2);
    memset(output, ' ', totalLength + 1);
    char *start = output + 1;
    for (int32_t i = 1; i < outputHeight; i++) {
        start[i * outputWidth - 1] = '\n';
    }
    drawTree(root, 1, 1, start, outputWidth);
    output[totalLength] = '\0';
    string result = output;
    free(output);
    return result;
}

void DistinctMapTest::drawTree(const TreeNode *root, int level, int idx, 
                           char *start, int32_t totalwidth) 
{
    int32_t count = 1 << (level - 1);
    int32_t width = totalwidth / count;
    char *offset = start + (level - 1) * 3 * totalwidth + (idx - 1) * width;
    docid_t id = INVALID_DOCID;
    if (root->_sdi) {
        id = root->_sdi->getDocId();
    }
    int32_t nw = width - 4;
    int32_t half = nw >> 1;
    if ((idx & 1) == 0) { //even
        half = nw - half;
    }
    sprintf(offset + half, "%03d", id);
    *(offset + strlen(offset))  = ' ';
    char *first = offset + (totalwidth + (half >> 1));
    char *mid = offset + totalwidth + half + 1;
    char *last = mid + (mid - first);
    if (root->_left) {
        memset(first + 1, '-', mid - first - 1);
        *first = '+';
        *mid = '+';
        *(first + totalwidth) = '|';
        drawTree(root->_left, level + 1, 2 * idx - 1, start, totalwidth);
    } 
    if (root->_right) {
        memset(mid + 1, '-', last - mid - 1);
        *mid = '+';
        *last = '+';
        *(last + totalwidth) = '|';
        drawTree(root->_right, level + 1, 2 * idx, start, totalwidth);
    }
}

void DistinctMapTest::fun(int n) {
    HA3_LOG(TRACE1, "%d nodes.", n);
    TreeNode nodes[n+1];
    for(int i = 1; i <= (n + 1)/ 2; i++) {
        if (2 * i <= n) {
            HA3_LOG(TRACE1, "%d -> %d", i, 2 * i);
            nodes[i]._left = &nodes[2 * i];
        }
        if (2 * i + 1 <= n) {
            HA3_LOG(TRACE1, "%d -> %d", i, 2 * i + 1);
            nodes[i]._right = &nodes[2 * i + 1];
        }
    }        
    HA3_LOG(DEBUG, "tree: %s", treeNodeToString(&nodes[1]).c_str());
}


DistinctInfo* DistinctMapTest::doInsert(docid_t docid, uint32_t key, float score) {
    DistinctInfo *item 
        = createFakeDistinctInfo(docid, key);
    _items.push_back(item);
    _dMap->insert(item);
    return item;
};

void DistinctMapTest::checkBalance(TreeNode *tn, const char *msg) {
    TreeNode *curNode = tn;
    if (!curNode) {
        return;
    }
    if (curNode->_left) {
        ASSERT_EQ(curNode, curNode->_left->_parent);
    }

    if (curNode->_right) {
        ASSERT_EQ(curNode, curNode->_right->_parent);
    }

    int32_t balance 
        = getTreeHeight(curNode->_right) - getTreeHeight(curNode->_left);
    HA3_LOG(DEBUG, "treenode_docid:%d", curNode->_sdi->getDocId());
    if (!(balance == (int32_t)curNode->_balance)) {
        HA3_LOG(ERROR, "assert failed, msg: %s", msg);
        assert(false);
    }
    checkBalance(curNode->_left, msg);
    checkBalance(curNode->_right, msg);
}

DistinctInfo* DistinctMapTest::
createFakeDistinctInfo(docid_t docid, int32_t key) {
    matchdoc::MatchDoc matchDoc = _allocator->allocate(docid);
    _keyRef->set(matchDoc, key);
    DistinctInfo *item = new DistinctInfo(matchDoc);
    return item;
}

void DistinctMapTest::deleteDistinctInfo(DistinctInfo *distInfo) {
    _allocator->deallocate((matchdoc::MatchDoc)distInfo->getMatchDoc());
    delete distInfo;
}

void DistinctMapTest::checkTreeNodeStatus(TreeNode *curNode, const char *msg) {
    if (curNode == NULL) {
        return;
    } else {
        DistinctInfo *distInfo = curNode->_sdi;
        uint32_t count = 0;
        while (distInfo) {
            if (!(curNode == distInfo->getTreeNode())) {
                HA3_LOG(ERROR, "assert failed, msg: %s", msg);
                assert(false);
            }
            distInfo = distInfo->_next;
            count ++;
        }
        if (!(curNode->getCount() == count)) {
            HA3_LOG(ERROR, "assert failed, msg: %s", msg);
            assert(false);
        }
        checkTreeNodeStatus(curNode->_left, msg);
        checkTreeNodeStatus(curNode->_right, msg);
    }
}

END_HA3_NAMESPACE(rank);

