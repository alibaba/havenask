#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/PartialIndexPartitionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/test/test.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <ha3/config/ConfigAdapter.h>
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/config/index_partition_options.h"
#include <indexlib/partition/partition_reader_snapshot.h>
#include "indexlib/partition/index_application.h"
#include <fslib/fslib.h>

using namespace std;
using namespace suez::turing;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(search);

class PartialIndexPartitionReaderWrapperTest : public TESTBASE {
public:
    PartialIndexPartitionReaderWrapperTest() {
        _indexRoot = GET_TEMP_DATA_PATH() + "PartialIndexPartitionReaderWrapperTest";
    };
    ~PartialIndexPartitionReaderWrapperTest() {};
public:
    void setUp();
    void tearDown();
protected:
    void checkLookup(PartialIndexPartitionReaderWrapper *wrapper,
                     const common::Term &term, df_t expectDF,
                     df_t expectWrapperDF = -1,
                     bool isSubPartition = false,
                     const LayerMeta *layerMeta = NULL,
                     PostingIteratorType expectPiType = pi_unknown);
protected:
    HA3_LOG_DECLARE();
private:
    string _indexRoot;
};

HA3_LOG_SETUP(search, PartialIndexPartitionReaderWrapperTest);

void PartialIndexPartitionReaderWrapperTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    fslib::fs::FileSystem::remove(_indexRoot);
}

void PartialIndexPartitionReaderWrapperTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    fslib::fs::FileSystem::remove(_indexRoot);
}

#define CREATE_TERM(type, value_name, word, index_name, truncate_name)  \
    type value_name(word, index_name, RequiredFields(), DEFAULT_BOOST_VALUE, truncate_name)

void PartialIndexPartitionReaderWrapperTest::checkLookup(
        PartialIndexPartitionReaderWrapper *wrapper,
        const Term &term, df_t expectDF, df_t expectWrapperDF,
        bool isSubPartition, const LayerMeta *layerMeta,
        PostingIteratorType expectPiType)
{
    if (-1 == expectWrapperDF) {
        expectWrapperDF = expectDF;
    }
    LookupResult result = wrapper->lookup(term, layerMeta);
    PostingIterator* iter = result.postingIt;
    if (expectDF == 0) {
        ASSERT_TRUE(!iter);
    } else {
        ASSERT_TRUE(iter);
        ASSERT_EQ(expectPiType, iter->GetType());
        ASSERT_EQ(expectDF, iter->GetTermMeta()->GetDocFreq());
        ASSERT_EQ(expectWrapperDF, wrapper->getTermDF(term));
    }
    ASSERT_EQ(isSubPartition, result.isSubPartition);
    if (isSubPartition) {
        ASSERT_TRUE(result.main2SubIt != NULL);
        ASSERT_TRUE(result.sub2MainIt != NULL);
        ASSERT_EQ(result.main2SubIt, wrapper->_main2SubIt);
        ASSERT_EQ(result.sub2MainIt, wrapper->_sub2MainIt);
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "abc:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    fakeIndex.indexes["bitmap_phrase"] = "abc:0;2;3;\n"
                                         "with:0;2;\n";
    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper.get(), term, 6);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper.get(), term, 6);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "BITMAP");
        checkLookup(wrapper.get(), term, 3, 3, false, NULL, pi_bitmap);
    }
    {
        CREATE_TERM(NumberTerm, term, 1, "number", "");
        checkLookup(wrapper.get(), term, 6);
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testLookupWithoutElement) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "abc:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    {
        CREATE_TERM(Term, term, "abcd", "phrase", "");
        checkLookup(wrapper.get(), term, 0);
    }
    {
        CREATE_TERM(Term, term, "abcd", "phrase", "");
        checkLookup(wrapper.get(), term, 0);
    }
    {
        CREATE_TERM(Term, term, "abcd", "phrase", "BITMAP");
        checkLookup(wrapper.get(), term, 0, 0, false, NULL);
    }
    {
        CREATE_TERM(NumberTerm, term, 3, "number", "");
        checkLookup(wrapper.get(), term, 0);
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testLookupInBitmap) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "bcd:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    fakeIndex.indexes["bitmap_phrase"] = "abc:0;2;3;\n"
                                         "with:0;2;\n";
    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper.get(), term, 3, 3, false, NULL, pi_bitmap);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "BITMAP");
        checkLookup(wrapper.get(), term, 3, 3, false, NULL, pi_bitmap);
    }
    {
        CREATE_TERM(NumberTerm, term, 2, "number", "");
        checkLookup(wrapper.get(), term, 6);
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testLookupForSubPartition) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["phrase"] = "bcd:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["sub_phrase"] = "abc:1;3;5;7;9;11;\n"
                                      "white:2;4;6;8;10;12;13;14;\n";
    fakeIndex.attributes = "main_docid_to_sub_docid_attr_name : int32_t : 4,8,12,14,17;"
                           "sub_docid_to_main_docid_attr_name : int32_t : 0,0,1,1,2,3,4;";
    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);

    {
        CREATE_TERM(Term, term, "abc", "sub_phrase", "");
        checkLookup(wrapper.get(), term, 6, 6, true);
    }
    {
        CREATE_TERM(Term, term, "white", "sub_phrase", "");
        checkLookup(wrapper.get(), term, 8, 8, true);
    }
    {
        CREATE_TERM(Term, term, "bcd", "sub_phrase", "");
        checkLookup(wrapper.get(), term, 0, 0, false);
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testGetTermKeyStr) {
    HA3_LOG(DEBUG, "Begin Test!");
    CREATE_TERM(Term, term, "abcd", "phrase", "BITMAP");
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "bcd:0;2;3;4;5;6;\n";
    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(
                fakeIndex);
    std::string keyStr;
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0,3));
    layerMeta.push_back(DocIdRangeMeta(4,5));
    wrapper->getTermKeyStr(term, NULL, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\tphrase\tBITMAP"));
    layerMeta.initRangeString();
    wrapper->getTermKeyStr(term, &layerMeta, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\tphrase\tBITMAP\tbegin: 0 end: 3 nextBegin: 0 quota: 0;begin: 4 end: 5 nextBegin: 4 quota: 0;"));
    term.setIndexName("");
    term.setTruncateName("");
    wrapper->getTermKeyStr(term, NULL, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\t\t"));
    wrapper->getTermKeyStr(term, &layerMeta, keyStr);
    ASSERT_EQ(keyStr, std::string("abcd\t\t\tbegin: 0 end: 3 nextBegin: 0 quota: 0;begin: 4 end: 5 nextBegin: 4 quota: 0;"));
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testGetAttributeMetaInfo) {
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

    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    ASSERT_EQ(size_t(4), wrapper->getIndexPartReaders().size());

}

TEST_F(PartialIndexPartitionReaderWrapperTest, testSingleSegment) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "5,5";
    fakeIndex.indexes["phrase"] = "abc:0;2;3;4;5;6;\n"
                                  "with:0;2;3;5;7;8;\n";
    fakeIndex.indexes["number"] = "1:0;2;3;4;5;6;\n"
                                  "2:0;2;3;5;7;8;\n";
    fakeIndex.indexes["bitmap_phrase"] = "abc:0;2;3;\n"
                                         "with:0;2;\n";
    PartialIndexPartitionReaderWrapperPtr wrapper =
        FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(fakeIndex);
    wrapper->setTopK(10);
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back({0, 4});
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper.get(), term, 6, 6, false, &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "");
        checkLookup(wrapper.get(), term, 6, 6, false,  &layerMeta);
    }
    {
        CREATE_TERM(Term, term, "abc", "phrase", "BITMAP");
        checkLookup(wrapper.get(), term, 3, 3, false, &layerMeta, pi_bitmap);
    }
    {
        CREATE_TERM(NumberTerm, term, 1, "number", "");
        checkLookup(wrapper.get(), term, 6, 6, false, &layerMeta);
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testMultiSegments) {
    IndexPartitionOptions options;
    string zoneName = "test_table";
    string field = "pk:uint64:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    schema->SetSchemaName(zoneName);
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("hf_dict", "A;C");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, _indexRoot));

    string fullDocs0 = "cmd=add,pk=0,string1=A,long1=0;"
                       "cmd=add,pk=1,string1=B,long1=1;"
                       "cmd=add,pk=2,string1=A,long1=2;"
                       "cmd=add,pk=3,string1=A,long1=3;"
                       "cmd=add,pk=4,string1=A,long1=4;"
                       "cmd=add,pk=5,string1=B,long1=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs0, "", ""));
    string incDocs1 = "cmd=add,pk=6,string1=A,long1=6,ts=6;"
                      "cmd=add,pk=7,string1=B,long1=7,ts=7;"
                      "cmd=add,pk=8,string1=A,long1=8,ts=8;"
                      "cmd=add,pk=9,string1=B,long1=9,ts=9;"
                      "cmd=add,pk=10,string1=A,long1=10,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "", ""));
    string rtDocs2 = "cmd=add,pk=11,string1=B,long1=11,ts=11;"
                     "cmd=add,pk=12,string1=B,long1=12,ts=12;"
                     "cmd=add,pk=13,string1=A,long1=13,ts=13;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs2, "", ""));
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({indexPart}, JoinRelationMap()));
    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(""));
    configAdapterPtr->_clusterConfigMapPtr.reset(new ClusterConfigMap);
    configAdapterPtr->_clusterConfigMapPtr->insert({zoneName, ClusterConfigInfo()});
    IndexPartitionWrapperPtr indexPartWrapper =
        IndexPartitionWrapper::createIndexPartitionWrapper(configAdapterPtr, indexPart, zoneName);
    ASSERT_TRUE(indexPartWrapper != NULL);

    auto wrapper = indexPartWrapper->createPartialReaderWrapper(snapshot);
    ASSERT_TRUE(wrapper != NULL);
    wrapper->setTopK(10);
    autil::mem_pool::Pool pool;
    {
        CREATE_TERM(Term, term, "A", "index1", "");
        checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                    term, 8, 8, false, NULL, pi_buffered);
        {//segment 0
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 5});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        4, 8, false, &layerMeta, pi_buffered);
        }
        {//segment 1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({6, 10});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        3, 8, false, &layerMeta, pi_buffered);
        }
        {//segment 2
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({11, 13});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        1, 8, false, &layerMeta, pi_buffered);
        }
        {// multi segments 0,1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 5});
            layerMeta.push_back({6, 10});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        7, 8, false, &layerMeta, pi_buffered);
        }
        {// multi segments 0,2
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 5});
            layerMeta.push_back({11, 13});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        5, 8, false, &layerMeta, pi_buffered);
        }
        {// empty layer meta, lookup fail
            LayerMeta layerMeta(&pool);
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        0, 0, false, &layerMeta, pi_buffered);
        }
        {// invalid segment sequence 1,0
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({6, 10});
            layerMeta.push_back({0, 5});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        0, 0, false, &layerMeta, pi_buffered);
        }
        {//partial segment 0
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 3});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        4, 8, false, &layerMeta, pi_buffered);
        }
        {//partial segment 1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({3, 5});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        4, 8, false, &layerMeta, pi_buffered);
        }
        {//cross segment 0,1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 8});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        7, 8, false, &layerMeta, pi_buffered);
        }
        {//invalid begin, negative number
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({-1, 13});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        8, 8, false, &layerMeta, pi_buffered);
        }
        {//invalid end, out of segmentcount
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 100});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        8, 8, false, &layerMeta, pi_buffered);
        }
        {//invalid end, out of segmentcount
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({11, 100});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        1, 8, false, &layerMeta, pi_buffered);
        }
        {//illegal range, begin > end
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({5, 3});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        0, 0, false, &layerMeta, pi_buffered);
        }
    }
    {//bitmap does not support partial lookup
        CREATE_TERM(Term, term, "A", "index1", "BITMAP");
        checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                    term, 8, 8, false, NULL, pi_bitmap);
        {//whole segment
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 5});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        8, 8, false, &layerMeta, pi_bitmap);
        }
        {//whole segment
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({6, 10});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        8, 8, false, &layerMeta, pi_bitmap);
        }
    }
    {//bitmap lookup fail
        CREATE_TERM(Term, term, "B", "index1", "BITMAP");
        checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                    term, 6, 6, false, NULL, pi_buffered);
        {//whole segment
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 5});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        2, 6, false, &layerMeta, pi_buffered);
        }
        {//whole segment
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({6, 10});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(), term,
                        2, 6, false, &layerMeta, pi_buffered);
        }
    }
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testPostingCache) {
    IndexPartitionOptions options;
    string field = "pk:uint64:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    string zoneName = "test_table";
    schema->SetSchemaName(zoneName);
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("hf_dict", "A;C");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, _indexRoot));

    string fullDocs0 = "cmd=add,pk=0,string1=A,long1=0;"
                       "cmd=add,pk=1,string1=B,long1=1;"
                       "cmd=add,pk=2,string1=A,long1=2;"
                       "cmd=add,pk=3,string1=A,long1=3;"
                       "cmd=add,pk=4,string1=A,long1=4;"
                       "cmd=add,pk=5,string1=B,long1=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs0, "", ""));
    string incDocs1 = "cmd=add,pk=6,string1=A,long1=6,ts=6;"
                      "cmd=add,pk=7,string1=B,long1=7,ts=7;"
                      "cmd=add,pk=8,string1=A,long1=8,ts=8;"
                      "cmd=add,pk=9,string1=B,long1=9,ts=9;"
                      "cmd=add,pk=10,string1=A,long1=10,ts=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "", ""));
    string rtDocs2 = "cmd=add,pk=11,string1=B,long1=11,ts=11;"
                     "cmd=add,pk=12,string1=B,long1=12,ts=12;"
                     "cmd=add,pk=13,string1=A,long1=13,ts=13;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs2, "", ""));
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({indexPart}, JoinRelationMap()));
    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(""));
    configAdapterPtr->_clusterConfigMapPtr.reset(new ClusterConfigMap);
    configAdapterPtr->_clusterConfigMapPtr->insert({zoneName, ClusterConfigInfo()});
    IndexPartitionWrapperPtr indexPartWrapper =
        IndexPartitionWrapper::createIndexPartitionWrapper(configAdapterPtr, indexPart, zoneName);
    ASSERT_TRUE(indexPartWrapper != NULL);

    auto wrapper = indexPartWrapper->createPartialReaderWrapper(snapshot);
    ASSERT_TRUE(wrapper != NULL);
    wrapper->setTopK(10);
    autil::mem_pool::Pool pool;
    CREATE_TERM(Term, term, "A", "index1", "");
    ASSERT_EQ(0, wrapper->_delPostingVec.size());
    ASSERT_EQ(0, wrapper->_postingCache.size());
    LookupResult resultAllSegments = wrapper->lookup(term, NULL);
    ASSERT_TRUE(resultAllSegments.postingIt != NULL);
    LayerMeta layerMeta(&pool);
    layerMeta.push_back({0, 5});
    layerMeta.initRangeString();
    ASSERT_EQ(1, wrapper->_delPostingVec.size());
    ASSERT_EQ(1, wrapper->_postingCache.size());
    LookupResult result = wrapper->lookup(term, &layerMeta);
    ASSERT_TRUE(result.postingIt != NULL);
    ASSERT_EQ(2, wrapper->_delPostingVec.size());
    ASSERT_EQ(2, wrapper->_postingCache.size());
    LookupResult result1 = wrapper->lookup(term, &layerMeta);//hit cache
    ASSERT_TRUE(result1.postingIt != NULL);
    ASSERT_EQ(3, wrapper->_delPostingVec.size());
    ASSERT_EQ(2, wrapper->_postingCache.size());

    layerMeta.pop_back();
    layerMeta.push_back({3, 4});
    layerMeta.initRangeString();
    LookupResult result12 = wrapper->lookup(term, &layerMeta);
    ASSERT_TRUE(result12.postingIt != NULL);
    ASSERT_EQ(4, wrapper->_delPostingVec.size());
    ASSERT_EQ(3, wrapper->_postingCache.size());

    layerMeta.pop_back();
    layerMeta.push_back({6, 10});
    layerMeta.initRangeString();
    LookupResult result2 = wrapper->lookup(term, &layerMeta);
    ASSERT_TRUE(result2.postingIt != NULL);
    ASSERT_EQ(5, wrapper->_delPostingVec.size());
    ASSERT_EQ(4, wrapper->_postingCache.size());

    layerMeta.pop_back();
    layerMeta.push_back({11, 13});
    layerMeta.initRangeString();
    LookupResult result3 = wrapper->lookup(term, &layerMeta);
    ASSERT_TRUE(result3.postingIt != NULL);
    ASSERT_EQ(6, wrapper->_delPostingVec.size());
    ASSERT_EQ(5, wrapper->_postingCache.size());

    //bitmap does not support partial lookup
    CREATE_TERM(Term, termbt, "A", "index1", "BITMAP");
    LookupResult bitmapResult = wrapper->lookup(termbt, NULL);
    ASSERT_TRUE(bitmapResult.postingIt != NULL);
    ASSERT_EQ(7, wrapper->_delPostingVec.size());
    ASSERT_EQ(6, wrapper->_postingCache.size());

    layerMeta.pop_back();
    layerMeta.push_back({0, 5});
    layerMeta.initRangeString();
    LookupResult result4 = wrapper->lookup(termbt, &layerMeta);
    ASSERT_TRUE(result4.postingIt != NULL);
    ASSERT_EQ(8, wrapper->_delPostingVec.size());
    ASSERT_EQ(7, wrapper->_postingCache.size());

    LookupResult result5 = wrapper->lookup(termbt, &layerMeta);//hit cache
    ASSERT_TRUE(result5.postingIt != NULL);
    ASSERT_EQ(9, wrapper->_delPostingVec.size());
    ASSERT_EQ(7, wrapper->_postingCache.size());

    layerMeta.pop_back();
    layerMeta.push_back({6, 10});
    layerMeta.initRangeString();
    LookupResult result6 = wrapper->lookup(termbt, &layerMeta);
    ASSERT_TRUE(result6.postingIt != NULL);
    ASSERT_EQ(10, wrapper->_delPostingVec.size());
    ASSERT_EQ(8, wrapper->_postingCache.size());
}

TEST_F(PartialIndexPartitionReaderWrapperTest, testMultiSegmentsSubPartition) {
    IndexPartitionOptions options;
    string field = "pk:uint64:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    string attr;
    string summary = "pk;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index,
            attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
                            "substr1:string;subpkstr:string;sub_long:uint32;",
                            "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
                            "substr1;subpkstr;sub_long;",
                            "");
    schema->SetSubIndexPartitionSchema(subSchema);
    string zoneName = "test_table";
    schema->SetSchemaName(zoneName);
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("hf_dict", "A;C");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, _indexRoot));

    string fullDocs0 = "cmd=add,pk=0,string1=A,subpkstr=sub0^sub1,substr1=s0^s1;"
                       "cmd=add,pk=1,string1=B,subpkstr=sub2^sub3,substr1=s2^s3;"
                       "cmd=add,pk=2,string1=A,subpkstr=sub4,substr1=s4;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs0, "pk:0",
                             "docid=0;"));
    string incDocs1 = "cmd=add,pk=3,string1=A,subpkstr=sub5,substr1=s1,ts=3;"
                      "cmd=add,pk=4,string1=B,subpkstr=sub6^sub7,substr1=s1^s7,ts=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs1, "", ""));
    string rtDocs2 = "cmd=add,pk=5,string1=B,subpkstr=sub8,substr1=s1,ts=5;"
                     "cmd=add,pk=6,string1=B,subpkstr=sub9^sub10,substr1=s1^s1,ts=6;"
                     "cmd=add,pk=7,string1=B,subpkstr=sub11^sub12,substr1=s1^s2,ts=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocs2, "pk:5", "docid=5"));
    IndexPartitionPtr indexPart = psm.GetIndexPartition();
    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({indexPart}, JoinRelationMap()));
    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(""));
    configAdapterPtr->_clusterConfigMapPtr.reset(new ClusterConfigMap);
    configAdapterPtr->_clusterConfigMapPtr->insert({zoneName, ClusterConfigInfo()});
    IndexPartitionWrapperPtr indexPartWrapper =
        IndexPartitionWrapper::createIndexPartitionWrapper(configAdapterPtr, indexPart, zoneName);
    ASSERT_TRUE(indexPartWrapper != NULL);

    auto wrapper = indexPartWrapper->createPartialReaderWrapper(snapshot);
    ASSERT_TRUE(wrapper != NULL);
    wrapper->setTopK(10);

    {
        CREATE_TERM(Term, term, "B", "index1", "");
        checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                    term, 5, 5, false, NULL, pi_buffered);
    }

    autil::mem_pool::Pool pool;
    {
        CREATE_TERM(Term, term, "s1", "subindex1", "");
        checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                    term, 7, 7, true, NULL, pi_buffered);
        { // segment 0
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 2});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 1, 7, true, &layerMeta, pi_buffered);
        }

        { // segment 0 partial
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({1, 2});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 1, 7, true, &layerMeta, pi_buffered);
        }

        { // segment 1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({3, 4});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 2, 7, true, &layerMeta, pi_buffered);
        }

        { // segment 2
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({5, 7});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 4, 7, true, &layerMeta, pi_buffered);
        }

        { // multi segments, 0,1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 2});
            layerMeta.push_back({3, 4});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 3, 7, true, &layerMeta, pi_buffered);
        }

        { // multi segments, 0,2
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 2});
            layerMeta.push_back({5, 7});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 5, 7, true, &layerMeta, pi_buffered);
        }

        { // multi segments, 0,1,2
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 2});
            layerMeta.push_back({3, 4});
            layerMeta.push_back({5, 7});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 7, 7, true, &layerMeta, pi_buffered);
        }

        { // invalid segment sequence, 2,0
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({5, 7});
            layerMeta.push_back({0, 2});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 0, 0, false, &layerMeta, pi_buffered);
        }

        { // invalid segment sequence, 2,0,1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({5, 7});
            layerMeta.push_back({0, 2});
            layerMeta.push_back({3, 4});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 0, 0, false, &layerMeta, pi_buffered);
        }

        { // invalid segment sequence, 0,2,1
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({0, 2});
            layerMeta.push_back({5, 7});
            layerMeta.push_back({3, 4});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 0, 0, false, &layerMeta, pi_buffered);
        }

        { // invalid begin
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({-1, 7});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 7, 7, true, &layerMeta, pi_buffered);
        }

        { // invalid end
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({3, 8});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 7, 7, true, &layerMeta, pi_buffered);
        }

        {// invalid range, begin > end
            LayerMeta layerMeta(&pool);
            layerMeta.push_back({7, 3});
            layerMeta.initRangeString();
            checkLookup((PartialIndexPartitionReaderWrapper*)wrapper.get(),
                        term, 0, 0, false, &layerMeta, pi_buffered);
        }
    }
}

END_HA3_NAMESPACE(search);
