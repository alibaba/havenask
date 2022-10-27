#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class IndexPartitionReaderWrapperTest : public TESTBASE {
public:
    IndexPartitionReaderWrapperTest();
    ~IndexPartitionReaderWrapperTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkLookup(const IndexPartitionReaderWrapperPtr &wrapper,
                     const common::Term &term, df_t expectDF,
                     bool isSubPartition = false, const LayerMeta *layerMeta = NULL);

protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, IndexPartitionReaderWrapperTest);


IndexPartitionReaderWrapperTest::IndexPartitionReaderWrapperTest() {
}

IndexPartitionReaderWrapperTest::~IndexPartitionReaderWrapperTest() {
}

void IndexPartitionReaderWrapperTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void IndexPartitionReaderWrapperTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

#define CREATE_TERM(type, value_name, word, index_name, truncate_name)  \
    type value_name(word, index_name, RequiredFields(), DEFAULT_BOOST_VALUE, truncate_name)

TEST_F(IndexPartitionReaderWrapperTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0,99));
    layerMeta.push_back(DocIdRangeMeta(100,199));
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "abc:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    fakeIndex.indexes["bitmap_phrase"] = "abc:0;2;3;\n"
                                         "with:0;2;\n";
    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper, term, 6);
        checkLookup(wrapper, term, 6, false, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper, term, 6);
        checkLookup(wrapper, term, 6, false, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "BITMAP");
        checkLookup(wrapper, term, 3);
        checkLookup(wrapper, term, 3, false, &layerMeta);
    }
    {
        CREATE_TERM(NumberTerm, term, 1, "number", "");
        checkLookup(wrapper, term, 6);
        checkLookup(wrapper, term, 6, false, &layerMeta);
    }
}

TEST_F(IndexPartitionReaderWrapperTest, testLookupWithoutElement) {
    HA3_LOG(DEBUG, "Begin Test!");
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0,99));
    layerMeta.push_back(DocIdRangeMeta(100,199));
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "abc:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    {
        CREATE_TERM(Term, term, "abcd", "phrase", "");
        checkLookup(wrapper, term, 0);
        checkLookup(wrapper, term, 0, false, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abcd", "phrase", "");
        checkLookup(wrapper, term, 0);
        checkLookup(wrapper, term, 0, false, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abcd", "phrase", "BITMAP");
        checkLookup(wrapper, term, 0);
        checkLookup(wrapper, term, 0, false, &layerMeta);
    }
    {
        CREATE_TERM(NumberTerm, term, 3, "number", "");
        checkLookup(wrapper, term, 0);
        checkLookup(wrapper, term, 0, false, &layerMeta);
    }
}

TEST_F(IndexPartitionReaderWrapperTest, testLookupInBitmap) {
    HA3_LOG(DEBUG, "Begin Test!");
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0,99));
    layerMeta.push_back(DocIdRangeMeta(100,199));
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "bcd:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    fakeIndex.indexes["bitmap_phrase"] = "abc:0;2;3;\n"
                                         "with:0;2;\n";
    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper, term, 3);
        checkLookup(wrapper, term, 3, false, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "BITMAP");
        checkLookup(wrapper, term, 3);
        checkLookup(wrapper, term, 3, false, &layerMeta);
    }
    {
        CREATE_TERM(NumberTerm, term, 2, "number", "");
        checkLookup(wrapper, term, 6);
        checkLookup(wrapper, term, 6, false, &layerMeta);
    }
}

TEST_F(IndexPartitionReaderWrapperTest, testLookupForSubPartition) {
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0,99));
    layerMeta.push_back(DocIdRangeMeta(100,199));
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["phrase"] = "bcd:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["sub_phrase"] = "abc:1;3;5;7;9;11;\n"
                                      "white:2;4;6;8;10;12;13;14;\n";
    fakeIndex.attributes = "main_docid_to_sub_docid_attr_name : int32_t : 4,8,12,14,17;"
                           "sub_docid_to_main_docid_attr_name : int32_t : 0,0,1,1,2,3,4;";
    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);

    {
        CREATE_TERM(Term, term, "abc", "sub_phrase", "");
        checkLookup(wrapper, term, 6, true);
        checkLookup(wrapper, term, 6, true, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "white", "sub_phrase", "");
        checkLookup(wrapper, term, 8, true);
        checkLookup(wrapper, term, 8, true, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "bcd", "sub_phrase", "");
        checkLookup(wrapper, term, 0, false);
        checkLookup(wrapper, term, 0, false, &layerMeta);
    }
}

void IndexPartitionReaderWrapperTest::checkLookup(
        const IndexPartitionReaderWrapperPtr &wrapper,
        const Term &term, df_t expectDF, bool isSubPartition,
        const LayerMeta *layerMeta)
{
    LookupResult result = wrapper->lookup(term, layerMeta);
    PostingIterator* iter = result.postingIt;
    if (expectDF == 0) {
        ASSERT_TRUE(!iter);
    } else {
        ASSERT_TRUE(iter);
        ASSERT_EQ(expectDF, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ(expectDF, wrapper->getTermDF(term));
    }
    ASSERT_EQ(isSubPartition, result.isSubPartition);
    if (isSubPartition) {
        ASSERT_TRUE(result.main2SubIt != NULL);
        ASSERT_TRUE(result.sub2MainIt != NULL);
        ASSERT_EQ(result.main2SubIt, wrapper->_main2SubIt);
        ASSERT_EQ(result.sub2MainIt, wrapper->_sub2MainIt);
    }
}

TEST_F(IndexPartitionReaderWrapperTest, testGetTermKeyStr) {
    HA3_LOG(DEBUG, "Begin Test!");
    CREATE_TERM(Term, term, "abcd", "phrase", "BITMAP");
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "bcd:0;2;3;4;5;6;\n";
    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    std::string keyStr;
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0,3));
    layerMeta.push_back(DocIdRangeMeta(4,5));
    wrapper->getTermKeyStr(term, NULL, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\tphrase\tBITMAP"));
    wrapper->getTermKeyStr(term, &layerMeta, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\tphrase\tBITMAP"));
    term.setIndexName("");
    term.setTruncateName("");
    wrapper->getTermKeyStr(term, NULL, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\t\t"));
    wrapper->getTermKeyStr(term, &layerMeta, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\t\t"));
}

TEST_F(IndexPartitionReaderWrapperTest, testGetAttributeMetaInfo) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.tablePrefix.push_back("join_");
    fakeIndex.tablePrefix.push_back("subjoin_");
    fakeIndex.indexes["join_pk"] = "0:0,1:1,2:3,3:5";
    fakeIndex.indexes["subjoin_pk"] = "1:0,3:1,5:2,7:4,9:5";
    fakeIndex.attributes = "main_attr1 : int32_t : 4,8,12,14,17;"
                           "main_attr2 : int32_t : 1,2,3,4,5;"
                           "joinfield : int32_t : 0,0,1,1,5;"
                           "sub_attr1 : int32_t : 1,2,3,4,5,6,7;"
                           "sub_attr2 : int32_t : 2,4,6,8,10,12,14;"
                           "sub_joinfield : int32_t : 1,3,5,7,9,11,13;"
                           "join_attr1 : int32_t : 3,6,9,12,15,18,21;"
                           "join_attr2 : int32_t : 4,8,12,16,20,24,28;"
                           "subjoin_attr1: int32_t : 1,2,3,4,5,6";

    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    ASSERT_EQ(size_t(4), wrapper->getIndexPartReaders().size());

}

TEST_F(IndexPartitionReaderWrapperTest, testGetPartedDocIdRanges) {
    {
        DocIdRangeVector rangeHint;
        size_t totalWayCount = 2;
        vector<DocIdRangeVector> rangeVectors;
        rangeHint.emplace_back(10, 70);
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        ASSERT_TRUE(wrapper->getPartedDocIdRanges(rangeHint, totalWayCount,
                        rangeVectors));
        ASSERT_EQ(2u, rangeVectors.size());
        ASSERT_EQ(1u, rangeVectors[0].size());
        ASSERT_EQ(make_pair(10, 40), rangeVectors[0][0]);
        ASSERT_EQ(1u, rangeVectors[1].size());
        ASSERT_EQ(make_pair(40, 70), rangeVectors[1][0]);

        wrapper->_indexPartReaderPtrs[IndexPartitionReaderWrapper::MAIN_PART_ID].reset();
        ASSERT_FALSE(wrapper->getPartedDocIdRanges(rangeHint, totalWayCount,
                        rangeVectors));
    }
    {
        DocIdRangeVector rangeHint;
        size_t totalWayCount = 2;
        DocIdRangeVector rangeVector;
        rangeHint.emplace_back(10, 70);
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        ASSERT_TRUE(wrapper->getPartedDocIdRanges(rangeHint, totalWayCount, 0,
                        rangeVector));
        ASSERT_EQ(1u, rangeVector.size());
        ASSERT_EQ(make_pair(10, 40), rangeVector[0]);
        ASSERT_TRUE(wrapper->getPartedDocIdRanges(rangeHint, totalWayCount, 1,
                        rangeVector));
        ASSERT_EQ(1u, rangeVector.size());
        ASSERT_EQ(make_pair(40, 70), rangeVector[0]);

        wrapper->_indexPartReaderPtrs[IndexPartitionReaderWrapper::MAIN_PART_ID].reset();
        ASSERT_FALSE(wrapper->getPartedDocIdRanges(rangeHint, totalWayCount, 0,
                        rangeVector));
        ASSERT_FALSE(wrapper->getPartedDocIdRanges(rangeHint, totalWayCount, 1,
                        rangeVector));
    }
}

TEST_F(IndexPartitionReaderWrapperTest, testGetSubRanges) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.attributes = "main_docid_to_sub_docid_attr_name : int32_t : 4,8,12,14,17;"
                           "sub_docid_to_main_docid_attr_name : int32_t : 0,0,1,1,2,3,4;";
    IndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    wrapper->makesureIteratorExist();
    autil::mem_pool::Pool pool;
    {
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(0,4));
        DocIdRangeVector subRanges;
        ASSERT_TRUE(wrapper->getSubRanges(&layerMeta, subRanges));
        ASSERT_EQ(1, subRanges.size());
        ASSERT_EQ(make_pair(0, 17), subRanges[0]);
    }
    {
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(0,0));
        DocIdRangeVector subRanges;
        ASSERT_TRUE(wrapper->getSubRanges(&layerMeta, subRanges));
        ASSERT_EQ(1, subRanges.size());
        ASSERT_EQ(make_pair(0, 4), subRanges[0]);
    }
    {
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(4,4));
        DocIdRangeVector subRanges;
        ASSERT_TRUE(wrapper->getSubRanges(&layerMeta, subRanges));
        ASSERT_EQ(1, subRanges.size());
        ASSERT_EQ(make_pair(14, 17), subRanges[0]);
    }
    {
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(0,0));
        layerMeta.push_back(DocIdRangeMeta(1,4));
        DocIdRangeVector subRanges;
        ASSERT_TRUE(wrapper->getSubRanges(&layerMeta, subRanges));
        ASSERT_EQ(2, subRanges.size());
        ASSERT_EQ(make_pair(0, 4), subRanges[0]);
        ASSERT_EQ(make_pair(4, 17), subRanges[1]);
    }
    {
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(0,0));
        layerMeta.push_back(DocIdRangeMeta(1,1));
        layerMeta.push_back(DocIdRangeMeta(2,2));
        layerMeta.push_back(DocIdRangeMeta(3,3));
        layerMeta.push_back(DocIdRangeMeta(4,4));
        DocIdRangeVector subRanges;
        ASSERT_TRUE(wrapper->getSubRanges(&layerMeta, subRanges));
        ASSERT_EQ(5, subRanges.size());
        ASSERT_EQ(make_pair(0, 4), subRanges[0]);
        ASSERT_EQ(make_pair(4, 8), subRanges[1]);
        ASSERT_EQ(make_pair(8, 12), subRanges[2]);
        ASSERT_EQ(make_pair(12, 14), subRanges[3]);
        ASSERT_EQ(make_pair(14, 17), subRanges[4]);
    }
    { //begin invalid
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(-1, 4));
        DocIdRangeVector subRanges;
        ASSERT_FALSE(wrapper->getSubRanges(&layerMeta, subRanges));
    }
    { //end invalid
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(0, 5));
        DocIdRangeVector subRanges;
        ASSERT_FALSE(wrapper->getSubRanges(&layerMeta, subRanges));
    }
    { //begin and end invalid
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(-1, 5));
        DocIdRangeVector subRanges;
        ASSERT_FALSE(wrapper->getSubRanges(&layerMeta, subRanges));
    }
    { //one invalid
        LayerMeta layerMeta(&pool);
        layerMeta.push_back(DocIdRangeMeta(0, 2));
        layerMeta.push_back(DocIdRangeMeta(3, 5));
        DocIdRangeVector subRanges;
        ASSERT_FALSE(wrapper->getSubRanges(&layerMeta, subRanges));
    }
}

END_HA3_NAMESPACE(search);
