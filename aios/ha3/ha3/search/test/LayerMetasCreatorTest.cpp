#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetasCreator.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3/search/test/LayerMetasConstructor.h>
#include <ha3/qrs/LayerClauseValidator.h>

using namespace std;
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class LayerMetasCreatorTest : public TESTBASE {
public:
    LayerMetasCreatorTest();
    ~LayerMetasCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void createLayerMetaTestTemplate(const std::string &layerStr, 
            const IE_NAMESPACE(index)::FakeIndex &fakeIndex,
            const std::string &expectStr, 
            bool isNewSyntax = false);
    void initQueryLayerClause(const std::string &layerStr);

protected:
    void docidLayerConvertTestTemplate(const std::string &resultRangeStr, 
            const std::string &totalDocCount, const std::string &layerStr);
    void orderedConvertTestTemplate(const std::string &resultRangeStr, 
                                    const IE_NAMESPACE(index)::FakeIndex &fakeIndex); 
    void unOrderedConvertTestTemplate(const std::string &resultRangeStr, 
            const IE_NAMESPACE(index)::FakeIndex &fakeIndex);
    void percentConvertTestTemplate(const std::string &lastRangeStr,
                                    const std::string &resultRangeStr,
                                    const std::string &layerStr);  
    void otherConvertTestTemplate(const std::string &lastRangeStr, 
                                  const std::string &visitedRangeStr,
                                  const std::string &totalDocCount,
                                  const std::string &resultRangeStr);
    void segmentLayerConvertTestTemplate(const std::string &resultRangeStr, 
            const std::string &totalDocCount, const std::string &layerStr);
    void convertSortAttrRangeTestTemplate(const std::string &resultRangeStr, 
            const std::string &layerStr, const std::string &lastRangeStr,
            const IE_NAMESPACE(index)::FakeIndex &fakeIndex, 
            bool hasRankHint);
    void interSectRangesTestTemplate(const std::string& layerMetaAStr, 
            const std::string& layerMetaBStr, const std::string& resultRangeStr);
    void initRankHintAndTestQuotaMode(const std::string& layerStr, QuotaMode &quotaMode); 
protected:
    static std::string deleteSpace(const std::string &inputString);
    static std::string convertLayerMetasToString(
            const LayerMetas *layerMetas);
    static void convertLayerMetaToString(
            const LayerMeta &layerMeta, std::stringstream &ss);
    static std::string convertLayerMetaToString(const LayerMeta &layerMeta);
protected:
    autil::mem_pool::Pool *_pool;
    common::QueryLayerClause *_layerClause;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, LayerMetasCreatorTest);


LayerMetasCreatorTest::LayerMetasCreatorTest() {
    _pool = new autil::mem_pool::Pool(1024);
    _layerClause = NULL;
}

LayerMetasCreatorTest::~LayerMetasCreatorTest() {    
    DELETE_AND_SET_NULL(_layerClause);
    delete _pool;
}

void LayerMetasCreatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void LayerMetasCreatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_layerClause);
}

TEST_F(LayerMetasCreatorTest, testCreateUnSortedLayer) {
    string layerStr = "range:%other,quota:888";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6|1";
    string expectStr = "(0,5,0),(6,6,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

TEST_F(LayerMetasCreatorTest, testCreateFullRange) {
    string layerStr = "quota:3000;"
                      "quota:4500";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "10,20";
    string expectStr = "(0,9,0),(10,29,0);"
                       "(0,9,0),(10,29,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

TEST_F(LayerMetasCreatorTest, testCreateSegmentidLayer) {
    string layerStr = "range:%segmentid{0},quota:3000;"
                      "range:%segmentid{[1,)};"
                      "range:%segmentid{[1,)},quota:UNLIMITED";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "10,20|30";
    string expectStr = "(0,9,0);"
                       "(10,29,0),(30,59,0);"
                       "(10,29,0),(30,59,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

TEST_F(LayerMetasCreatorTest, testCreateSegmentidLayer2) {
    string layerStr = "range:%segmentid{0},quota:3000;"
                      "range:%segmentid{[1,)};"
                      "range:%segmentid{2,1},quota:UNLIMITED";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "10,20|30";
    string expectStr = "(0,9,0);"
                       "(10,29,0),(30,59,0);"
                       "(10,29,0),(30,59,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

TEST_F(LayerMetasCreatorTest, testSimpleProcess) {
    string layerStr = "range:dimen0{1,3}*dimen1{32.3, [2.2,10.8]},quota:888;"
                      "range:%docid{1,6,[3,5)},quota:999;"
                      "range:%other";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6|1";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    string expectStr = "(0,0,0),(1,1,0),(5,5,0);"
                       "(1,1,0),(3,4,0),(6,6,0);"
                       "(2,2,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

TEST_F(LayerMetasCreatorTest, testFirstAttrIsRange) {
    string layerStr = "range:dimen0{[1,3]}*dimen1{32.3, [2.2,10.8]},quota:800";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6|10";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    string expectStr = "(0,5,0),(6,15,0)#meta:800,PER_LAYER;";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

// test %sorted,%unsorted,%percent etc
TEST_F(LayerMetasCreatorTest, testNewSyntaxCreateFullRange) {
    string layerStr = "quota:3000;"
                      "range:%unsorted;"
                      "quota:4500";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "10,20|30";
    string expectStr = "(0,9,0),(10,29,0),(30,59,0);"
                       "(30,59,0)#meta:0,PER_LAYER;"
                       "(0,9,0),(10,29,0),(30,59,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr, true);
}

TEST_F(LayerMetasCreatorTest, testNewSyntaxSimpleProcess) {
    string layerStr = "range:dimen0{1,3}*dimen1{32.3, [,10.8]},quota:888;"
                      "range:%docid{1,6,[3,5)},quota:999;"
                      "range:%other";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6|1";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    string expectStr = "(0,0,0),(1,1,0),(5,5,0);"
                       "(1,1,0),(3,4,0),(6,6,0);"
                       "(2,2,0);"
                       "(6,6,0)#meta:4294967295,PER_LAYER;";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr, true);
}

TEST_F(LayerMetasCreatorTest, testNewSyntaxSimpleProcessWithSorted) {
    string layerStr = "range:%sorted*dimen0{3,1}*dimen1{32.3, [2.2,10.8]},quota:888;"
                      "range:%docid{1,6,[3,5)},quota:999;"
                      "range:%other";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6|1";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    string expectStr = "(0,0,0),(1,1,0),(5,5,0);"
                       "(1,1,0),(3,4,0),(6,6,0);"
                       "(2,2,0);";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr, true);
}

TEST_F(LayerMetasCreatorTest, testNewSyntaxPercent) {
    string layerStr = "range:dimen0{1,3}*dimen1{32.3, [2.2,10.8]}*%percent{[50,100)},quota:888;"
                      "range:%docid{1,6,[3,5)},quota:999;"
                      "range:%other";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6|1";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    string expectStr = "(5,5,0);"
                       "(1,1,0),(3,4,0),(6,6,0);"
                       "(0,0,0),(2,2,0);"
                       "(6,6,0)#meta:4294967295,PER_LAYER;";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr, true);
}

TEST_F(LayerMetasCreatorTest, testNewSyntaxSimpleProcessOther) {
    {
        string layerStr = "range:dimen0{1,3}*dimen1{32.3, [2.2,10.8]},quota:888;"
                          "range:%unsorted,quota:UNLIMITED;"
                          "range:%docid{1,6,[3,5)},quota:999;"
                          "range:%other";
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "6|1";
        fakeIndex.partitionMeta = "+dimen0,+dimen1";
        fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                               "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
        string expectStr = "(0,0,0),(1,1,0),(5,5,0);"
                           "(6,6,0)#meta:4294967295,PER_LAYER;"
                           "(1,1,0),(3,4,0),(6,6,0);"
                           "(2,2,0);";
        createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr, true);
    }
    {
        string layerStr = "range:dimen0{1,3}*dimen1{32.3, [2.2,10.8]},quota:888;"
                          "range:%unsorted;"
                          "range:%docid{1,6,[3,5)},quota:999;"
                          "range:%other";
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "6|1";
        fakeIndex.partitionMeta = "+dimen0,+dimen1";
        fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                               "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
        string expectStr = "(0,0,0),(1,1,0),(5,5,0);"
                           "(6,6,0)#meta:0,PER_LAYER;"
                           "(1,1,0),(3,4,0),(6,6,0);"
                           "(2,2,0);";
        createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr, true);
    }
}

void LayerMetasCreatorTest::createLayerMetaTestTemplate(const string &layerStr, 
        const FakeIndex &fakeIndex, const string &expectStr, bool isNewSyntax) 
{
    ClauseParserContext ctx;
    initQueryLayerClause(layerStr);
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    if (isNewSyntax) {
        LayerClauseValidator validator(NULL);
        validator.autoAddSortedRange(_layerClause);
    }
    LayerMetasCreator creator;
    creator.init(_pool, wrapper.get());
    LayerMetas *layerMetas = creator.create(_layerClause);
    string trimdExpectStr = deleteSpace(expectStr);    
    ASSERT_EQ(trimdExpectStr, 
                         convertLayerMetasToString(
                                 layerMetas));
    POOL_DELETE_CLASS(layerMetas);
}

////////////////////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testConvertNumberToRange) {
    ASSERT_EQ(11, LayerMetasCreator::convertNumberToRange("11", 1000));
    ASSERT_EQ(1000, LayerMetasCreator::convertNumberToRange("1100", 1000));
    ASSERT_EQ(1000, LayerMetasCreator::convertNumberToRange("INFINITE", 1000));
}

///////////////////////////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testOtherConvert) {
    {
        string lastRangeStr = "0,50,0;"
                              "70,90,0;";
        string visitedRangeStr = "20,60,0";
        string totalDocCount = "100";
        string resultRangeStr = "(0,19,0),(70,90,0)";
        otherConvertTestTemplate(lastRangeStr, visitedRangeStr, totalDocCount, resultRangeStr);
    }
    {
        string lastRangeStr = "0,100,0;";
        string visitedRangeStr = "20,60,0";
        string totalDocCount = "100";
        string resultRangeStr = "(0,19,0),(61,99,0)";
        otherConvertTestTemplate(lastRangeStr, visitedRangeStr, totalDocCount, resultRangeStr);       
    }
    {
        string lastRangeStr = "0,50,0;"
                              "51,100,0";
        string visitedRangeStr = "0,100,0";
        string totalDocCount = "100";
        string resultRangeStr = "";
        otherConvertTestTemplate(lastRangeStr, visitedRangeStr, totalDocCount, resultRangeStr);       
    }
}

void LayerMetasCreatorTest::otherConvertTestTemplate(const string &lastRangeStr, 
        const string &visitedRangeStr, const string &totalDocCount,
        const string &resultRangeStr)
{
    LayerMeta lastRange = LayerMetasConstructor::createLayerMeta(
            _pool, lastRangeStr);
    LayerMeta visitedRange = LayerMetasConstructor::createLayerMeta(
            _pool, visitedRangeStr);
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = totalDocCount;
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    LayerMetasCreator creator;
    creator.init(_pool, wrapper.get(), NULL);
    LayerMeta resultRange = creator.otherConvert(lastRange, visitedRange);
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}

////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testPercentConvert) {
    {
        string layerStr = "range:%percent{[0,50)}";
        string lastRangeStr = "0,100,0;"
                              "300,400,0;";
        string resultRangeStr = "(0,100,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[50,100)}";
        string lastRangeStr = "0,199,0;"
                              "300,399,0;";
        string resultRangeStr = "(150,199,0),(300,399,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[50,)}";
        string lastRangeStr = "0,199,0;"
                              "300,399,0;";
        string resultRangeStr = "(150,199,0),(300,399,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[40,60),[0,20)}";
        string lastRangeStr = "0,99,0;"
                              "300,399,0,";
        string resultRangeStr = "(0,39,0),(80,99,0),(300,319,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[0,33),[33,66),[66,100)}";
        string lastRangeStr = "0,99,0;"
                              "300,399,0,";
        string resultRangeStr = "(0,65,0),(66,99,0),(300,331,0),(332,399,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[40,20)}";
        string lastRangeStr = "0,100,0;"
                              "300,400,0;";
        string resultRangeStr = "";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[0,50)}";
        string lastRangeStr = "0,0,0;"
                              "1,1,0;"
                              "6,6,0;";
        string resultRangeStr = "(0,0,0),(1,1,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[50,100)}";
        string lastRangeStr = "0,0,0;"
                              "1,1,0;"
                              "6,6,0;";
        string resultRangeStr = "(6,6,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[50,100)}";
        string lastRangeStr = "0,0,0;"
                              "1,1,0;"
                              "6,6,0;"
                              "7,7,0";
        string resultRangeStr = "(6,6,0),(7,7,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
    {
        string layerStr = "range:%percent{[0,50)}";
        string lastRangeStr = "0,0,0;"
                              "1,1,0;"
                              "6,6,0;"
                              "7,7,0";
        string resultRangeStr = "(0,0,0),(1,1,0)";
        percentConvertTestTemplate(lastRangeStr, resultRangeStr, layerStr);    
    }
}

void LayerMetasCreatorTest::percentConvertTestTemplate(const string &lastRangeStr,
        const string &resultRangeStr, const string &layerStr) 
{
    LayerMeta lastRange = LayerMetasConstructor::createLayerMeta(
            _pool, lastRangeStr);
    initQueryLayerClause(layerStr);
    LayerKeyRange *range = _layerClause->getLayerDescription(0)->getLayerRange(0);
    LayerMetasCreator creator;
    creator.init(_pool, NULL, NULL);
    LayerMeta resultRange = creator.percentConvert(lastRange, range);
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}

////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testUnorderedConvert) {
    {
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "1,2|3";
        string resultRangeStr = "(3,5,0)";
        unOrderedConvertTestTemplate(resultRangeStr, fakeIndex);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "1,2";
        string resultRangeStr = "";
        unOrderedConvertTestTemplate(resultRangeStr, fakeIndex);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "|3";
        string resultRangeStr = "(0,2,0)";
        unOrderedConvertTestTemplate(resultRangeStr, fakeIndex);
    }
}

void LayerMetasCreatorTest::unOrderedConvertTestTemplate(
        const string &resultRangeStr, const FakeIndex &fakeIndex) 
{
    LayerMetasCreator creator;
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    creator.init(_pool, wrapper.get(), NULL);
    LayerMeta resultRange = creator.unOrderedDocIdConvert();
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}

/////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testOrderedConvert) {
    {
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "1,2|3";
        string resultRangeStr = "(0,0,0),(1,2,0)";
        orderedConvertTestTemplate(resultRangeStr, fakeIndex);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "1,2";
        string resultRangeStr = "(0,0,0),(1,2,0)";
        orderedConvertTestTemplate(resultRangeStr, fakeIndex);
    }
    {
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "|3";
        string resultRangeStr = "";
        orderedConvertTestTemplate(resultRangeStr, fakeIndex);
    }
}

void LayerMetasCreatorTest::orderedConvertTestTemplate(const string &resultRangeStr, 
        const FakeIndex &fakeIndex) 
{
    LayerMetasCreator creator;
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    creator.init(_pool, wrapper.get(), NULL);
    LayerMeta resultRange = creator.orderedDocIdConvert();
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
} 

/////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testDocidLayerConvert) {
    {
        string layerStr = "range:%docid{[0,51),62,88}";
        string totalDocCount = "100";
        string resultRangeStr = "(0,50,0),(62,62,0),(88,88,0)";
        docidLayerConvertTestTemplate(resultRangeStr, totalDocCount, layerStr);
    }
    {
        string layerStr = "range:%docid{[0,51),62,33}";
        string totalDocCount = "100";
        string resultRangeStr = "(0,50,0),(62,62,0)";
        docidLayerConvertTestTemplate(resultRangeStr, totalDocCount, layerStr);
    }
    {
        string layerStr = "range:%docid{[20,51),62,10}";
        string totalDocCount = "100";
        string resultRangeStr = "(10,10,0),(20,50,0),(62,62,0)";
        docidLayerConvertTestTemplate(resultRangeStr, totalDocCount, layerStr);
    }
}

void LayerMetasCreatorTest::docidLayerConvertTestTemplate(const string &resultRangeStr, 
        const string &totalDocCount, const string &layerStr)
{
    initQueryLayerClause(layerStr);
    LayerKeyRange *range = _layerClause->getLayerDescription(0)->getLayerRange(0);
    LayerMetasCreator creator;
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = totalDocCount;
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    creator.init(_pool, wrapper.get(), NULL);
    LayerMeta resultRange = creator.docidLayerConvert(range);
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}

/////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testSegmentLayerConvert) {
    {
        string layerStr = "range:%segmentid{[0,1)}";
        string segmentDocCounts = "3,4";
        string resultRangeStr = "(0,2,0)";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
    {
        string layerStr = "range:%segmentid{[0,1),[2,)}";
        string segmentDocCounts = "3,4,5,6";
        string resultRangeStr = "(0,2,0),(7,17,0)";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
    {
        string layerStr = "range:%segmentid{[2,),[0,1)}";
        string segmentDocCounts = "3,4,5,6";
        string resultRangeStr = "(0,2,0),(7,17,0)";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
    {
        string layerStr = "range:%segmentid{[1,)}";
        string segmentDocCounts = "3,4";
        string resultRangeStr = "(3,6,0)";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
    {
        string layerStr = "range:%segmentid{[0,)}";
        string segmentDocCounts = "3,4|2";
        string resultRangeStr = "(0,8,0)";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
    {
        string layerStr = "range:%segmentid{[5,)}";
        string segmentDocCounts = "3,4|2";
        string resultRangeStr = "";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
    {
        string layerStr = "range:%segmentid{[,2)}";
        string segmentDocCounts = "3,4|2";
        string resultRangeStr = "(0,6,0)";
        segmentLayerConvertTestTemplate(resultRangeStr, segmentDocCounts, layerStr);
    }
}

void LayerMetasCreatorTest::segmentLayerConvertTestTemplate(const string &resultRangeStr, 
        const string &totalDocCount, const string &layerStr)
{
    initQueryLayerClause(layerStr);
    LayerKeyRange *range = _layerClause->getLayerDescription(0)->getLayerRange(0);
    LayerMetasCreator creator;
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = totalDocCount;
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    creator.init(_pool, wrapper.get(), NULL);
    LayerMeta resultRange = creator.segmentidLayerConvert(range);
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}

/////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testSortAttrRangeConvert) {
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    {
        string layerStr = "range:dimen0{1,2}*dimen1{[2.2,3.5]}";
        string resultRangeStr = "(0,0,0),(2,3,0)";
        string lastRangeStr = "0,5,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr, 
                fakeIndex, false);
    }
    {
        string layerStr = "range:dimen0{1,2}*dimen1{[2.2,3.5]}";
        string resultRangeStr = "(0,0,0),(2,2,0)";
        string lastRangeStr = "0,2,0;4,5,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr, 
                fakeIndex, false);
    }
    {
        string layerStr = "range:dimen0{1,2}*dimen1{[2.2,)}";
        string resultRangeStr = "(0,1,0),(2,3,0)";
        string lastRangeStr = "0,5,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr, 
                fakeIndex, false);
    }
    {
        string layerStr = "range:dimen1{[2.2,)}*dimen0{1,2}";
        string resultRangeStr = "(0,5,0)#meta:0,PER_LAYER";
        string lastRangeStr = "0,5,0";
        QuotaMode quotaMode = QM_PER_DOC;
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr, 
                fakeIndex, false);
        ASSERT_EQ(QM_PER_DOC, quotaMode);
    }
    {
        string layerStr = "range:dimen0{3,2}*dimen1{[10,),2}";
        string resultRangeStr = "(4,5,0)";
        string lastRangeStr = "0,5,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr, 
                fakeIndex, false);
    }
    {
        string layerStr = "range:dimen0{3,2}*dimen1{[2.2,3.3]}";
        string resultRangeStr = "";
        string lastRangeStr = "8,10,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr,
                fakeIndex, false);
    }
    {
        string layerStr = "range:dimen0{1,2}*dimen1{[2.2,3.5]}";
        string resultRangeStr = "(0,0,0),(2,3,0)";
        string lastRangeStr = "0,5,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr,
                fakeIndex, true);
    }
    {
        string layerStr = "quota:1000";
        QuotaMode quotaMode;
        initRankHintAndTestQuotaMode(layerStr, quotaMode);

    }
    {
        string layerStr = "range:%docid{3,4}";
        QuotaMode quotaMode;
        initRankHintAndTestQuotaMode(layerStr, quotaMode);
    }
    {
        string layerStr = "range:%segmentid{0}";
        QuotaMode quotaMode;
        initRankHintAndTestQuotaMode(layerStr, quotaMode);
    }
    {
        string layerStr = "range:%unsorted";
        QuotaMode quotaMode;
        initRankHintAndTestQuotaMode(layerStr, quotaMode);
    }
    {
        string layerStr = "range:dimen0{1,2}*dimen1{[2.2,3.5]}";
        QuotaMode quotaMode = QM_PER_DOC;
        initRankHintAndTestQuotaMode(layerStr, quotaMode);
    }
    {
        FakeIndex fakeIndex1;
        fakeIndex1.segmentDocCounts = "6";
        fakeIndex1.partitionMeta = "+dimen0,+dimen2";
        fakeIndex1.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                                "dimen2 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
        string layerStr = "range:dimen0{1,2}*dimen2{[2.2,3.5]}";
        string resultRangeStr = "(0,0,0),(2,3,0)";
        string lastRangeStr = "0,5,0";
        convertSortAttrRangeTestTemplate(resultRangeStr, layerStr, lastRangeStr,
                fakeIndex1, true);
    }
}

void LayerMetasCreatorTest::convertSortAttrRangeTestTemplate(const string &resultRangeStr, 
        const string &layerStr, const string &lastRangeStr, 
        const FakeIndex& fakeIndex, bool hasRankHint)
{
    initQueryLayerClause(layerStr);
    LayerDescription *layerDesc = _layerClause->getLayerDescription(0);
    int32_t from = 0;
    int32_t to = layerDesc->getLayerRangeCount() - 1;
    LayerMeta lastRange = LayerMetasConstructor::createLayerMeta(
            _pool, lastRangeStr);
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    RankHint rankHint;
    rankHint.sortField = "dimen2";
    RankHint *pRankHint = hasRankHint ? &rankHint : NULL;
    LayerMetasCreator creator;
    creator.init(_pool, wrapper.get(), pRankHint);
    LayerMeta resultRange = creator.convertSortAttrRange(layerDesc, from, to, 
            lastRange);
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}

void LayerMetasCreatorTest::initRankHintAndTestQuotaMode(const string& layerStr, 
        QuotaMode &quotaMode) 
{
    initQueryLayerClause(layerStr);
    quotaMode = QM_PER_DOC;
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "6";
    fakeIndex.partitionMeta = "+dimen0,+dimen1";
    fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                           "dimen1 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
    IndexPartitionReaderWrapperPtr wrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    RankHint rankHint;
    rankHint.sortField = "dimen2";
    RankHint *pRankHint = &rankHint;
    LayerMetasCreator creator;
    creator.init(_pool, wrapper.get(), pRankHint);
    LayerMetas *layerMetas = creator.createLayerMetas(_layerClause);
    if (!(QM_PER_LAYER == layerMetas->back().quotaMode)) {
        HA3_LOG(ERROR, "layerStr is [%s]", layerStr.c_str());
        assert(false);
    }
}

/////////////////////////////////////////////////////////////

TEST_F(LayerMetasCreatorTest, testInterSectRanges) {
    {
        string layerMetaA = "0,3,0;3,5,0;5,7,0";
        string layerMetaB = "2,4,0;6,10,0;13,14,0;16,18,0";
        string resultStr = "(2,3,0),(3,4,0),(6,7,0)";

        interSectRangesTestTemplate(layerMetaA, layerMetaB, resultStr);
    }
    {
        string layerMetaA = "";
        string layerMetaB = "2,4,0;6,10,0;13,14,0;16,18,0";
        string resultStr = "";

        interSectRangesTestTemplate(layerMetaA, layerMetaB, resultStr);
    }
}

TEST_F(LayerMetasCreatorTest, testQuotaType) {
    string layerStr = "quota:3000#1;"
                      "quota:4500#2";
    FakeIndex fakeIndex;
    fakeIndex.segmentDocCounts = "10,20";
    string expectStr = "(0,9,0),(10,29,0)#type:3000,QT_AVERAGE;"
                       "(0,9,0),(10,29,0)#type:9000,QT_AVERAGE;";
    createLayerMetaTestTemplate(layerStr, fakeIndex, expectStr);
}

void LayerMetasCreatorTest::interSectRangesTestTemplate(const string& layerMetaAStr, 
        const string& layerMetaBStr, const string& resultRangeStr) 
{
    LayerMeta layerMetaA = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaAStr);
    LayerMeta layerMetaB = LayerMetasConstructor::createLayerMeta(
            _pool, layerMetaBStr);
    LayerMetasCreator creator;
    creator.init(_pool, NULL, NULL);
    LayerMeta resultRange = creator.interSectRanges(layerMetaA, layerMetaB);
    ASSERT_EQ(resultRangeStr, convertLayerMetaToString(resultRange));
}


/////////////////////////////////////////////////////////////

void LayerMetasCreatorTest::initQueryLayerClause(const string &layerStr) {
    DELETE_AND_SET_NULL(_layerClause);
    ClauseParserContext ctx;
    if (!ctx.parseLayerClause(layerStr.c_str())) {
        HA3_LOG(ERROR, "msg is [%s]", layerStr.c_str());
        assert(false);
    }
    _layerClause = ctx.stealLayerClause();
}

string LayerMetasCreatorTest::deleteSpace(const string &inputString) {
    string outputString;
    outputString.reserve(inputString.size());
    string::const_iterator it = inputString.begin();
    for (; it != inputString.end(); ++it) {
        if (*it != ' ') {
            outputString.append(1, *it);
        }
    }
    return outputString;
}

string LayerMetasCreatorTest::convertLayerMetasToString(
        const LayerMetas *layerMetas)
{
    stringstream ss;
    for(size_t i = 0; i < layerMetas->size(); i++) {
        convertLayerMetaToString((*layerMetas)[i], ss);
        ss << ';';
    }
    return ss.str();
}

string LayerMetasCreatorTest::convertLayerMetaToString(
        const LayerMeta &layerMeta)
{
    stringstream ss;
    convertLayerMetaToString(layerMeta, ss);
    return ss.str();
}

void LayerMetasCreatorTest::convertLayerMetaToString(
        const LayerMeta &layerMeta, stringstream &ss)
{
    for(size_t i = 0; i < layerMeta.size(); i++) {
        const DocIdRangeMeta &meta = layerMeta[i];
        if (i != 0) {
            ss << ',';
        }
        ss << '(' << meta.begin << ',' << meta.end << ',' << meta.quota << ')'; 
    }
    QuotaMode quotaMode = layerMeta.quotaMode;
    uint32_t quota = layerMeta.quota;
    if (quotaMode == QM_PER_LAYER) {
        ss << "#meta:" << quota << ',' << "PER_LAYER";
    }
    if (layerMeta.quotaType == QT_AVERAGE) {
        ss << "#type:" << quota  << ",QT_AVERAGE";
    }
    if (layerMeta.quotaType == QT_QUOTA) {
        ss << "#type:" << quota  << ",QT_QUOTA";
    }
}
END_HA3_NAMESPACE(search);

