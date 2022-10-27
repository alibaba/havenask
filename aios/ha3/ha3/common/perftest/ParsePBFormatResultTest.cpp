#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hits.h>
#include <ha3/common/PBResultFormatter.h>
#include <sstream>
#include <autil/ZlibCompressor.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(common);

class ParsePBFormatResultTest : public TESTBASE {
public:
    ParsePBFormatResultTest();
    ~ParsePBFormatResultTest();
protected:
    ParsePBFormatResultTest(const ParsePBFormatResultTest &);
    ParsePBFormatResultTest& operator=(const ParsePBFormatResultTest &);
public:
    void setUp();
    void tearDown();
protected:
    Hits* createHits(uint32_t hitNum, uint32_t attributeNum);
    void testParseResult(uint32_t hitNum, uint32_t attrbiuteNum);
    void testParseResult(uint32_t hitNum, uint32_t attrbiuteNum, int type);

    void fillNewPbResult(PBResult pbResult, PBResult &newPbResult);
    void fillNewPbResultVariableValues(PBResult pbResult,
            PBMatchDocs* pbMatchDocs);
    void fillNewPbResultAttributes(PBResult pbResult,
                                   PBMatchDocs* pbMatchDocs);
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ParsePBFormatResultTest);

HA3_LOG_SETUP(perftest, ParsePBFormatResultTest);


ParsePBFormatResultTest::ParsePBFormatResultTest() { 
}

ParsePBFormatResultTest::~ParsePBFormatResultTest() { 
}

void ParsePBFormatResultTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ParsePBFormatResultTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ParsePBFormatResultTest, testSimpleProcess) {
    testParseResult(10, 2);
    testParseResult(10, 5);
    testParseResult(10, 10);

    testParseResult(1000, 2);
    testParseResult(1000, 5);
    testParseResult(1000, 10);
}

TEST_F(ParsePBFormatResultTest, testPBWithZlibDefault) {
    testParseResult(10, 2, Z_DEFAULT_COMPRESSION);
    testParseResult(10, 5, Z_DEFAULT_COMPRESSION);
    testParseResult(10, 10,Z_DEFAULT_COMPRESSION);

    testParseResult(1000, 2, Z_DEFAULT_COMPRESSION);
    testParseResult(1000, 5,Z_DEFAULT_COMPRESSION);
    testParseResult(1000, 10,Z_DEFAULT_COMPRESSION);
}
TEST_F(ParsePBFormatResultTest, testPBWithZlibSpeed) {
    testParseResult(10, 2, Z_BEST_SPEED);
    testParseResult(10, 5, Z_BEST_SPEED);
    testParseResult(10, 10,Z_BEST_SPEED);

    testParseResult(1000, 2, Z_BEST_SPEED);
    testParseResult(1000, 5, Z_BEST_SPEED);
    testParseResult(1000, 10,Z_BEST_SPEED);
}

void ParsePBFormatResultTest::testParseResult(uint32_t hitNum, uint32_t attributeNum) { 
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t toTime = 0;
    uint64_t parseTime = 0;
    uint64_t totalSize = 0;
    for (uint32_t i = 0; i < 1000; ++i) {
        stringstream ss;    
        Hits* hits = createHits(hitNum, attributeNum);
        ResultPtr result(new Result);
        result->setHits(hits);
        
        PBResultFormatter pbResultFormatter;
        uint64_t toStartTime = TimeUtility::currentTime();
        pbResultFormatter.format(result, ss);
        uint64_t toEndTime = TimeUtility::currentTime();
        toTime = toTime + toEndTime - toStartTime;
        totalSize += ss.str().size();

        PBResult pbResult;        
        uint64_t startTime = TimeUtility::currentTime();    
        pbResult.ParseFromIstream(&ss);
        uint64_t endTime = TimeUtility::currentTime();
        
        parseTime = parseTime + endTime - startTime;
    }

    // HA3_LOG(ERROR, "Parse PB FormatResult performance:\n toTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%d B)", 
    //         toTime / 1000, parseTime / 1000, hitNum, attributeNum, totalSize / 1000);
    printf("Parse PB FormatResult performance:\ntoTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%lu B)\n", 
           toTime / 1000, parseTime / 1000, hitNum, attributeNum, totalSize / 1000);
}

void ParsePBFormatResultTest::testParseResult(uint32_t hitNum, uint32_t attributeNum, int type) { 
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t parseTime = 0;
    uint64_t totalSize = 0;
    uint64_t toTime = 0;
    
    for (uint32_t i = 0; i < 1000; ++i) {
        stringstream ss;
        Hits* hits = createHits(hitNum, attributeNum);
        ResultPtr result(new Result);
        result->setHits(hits);
        ZlibCompressor zlibCompressor(type);
        PBResultFormatter pbResultFormatter;
        uint64_t toStartTime = TimeUtility::currentTime();
        pbResultFormatter.format(result, ss);
        zlibCompressor.addDataToBufferIn(ss.str());
        bool ret = zlibCompressor.compress();
        std::string compressedResult(zlibCompressor.getBufferOut(), zlibCompressor.getBufferOutLen());
        uint64_t toEndTime = TimeUtility::currentTime();
        toTime = toTime + toEndTime - toStartTime;

        PBResult pbResult;        
        uint64_t startTime = TimeUtility::currentTime();    
        ZlibCompressor zlibCompressor1(type);
        zlibCompressor1.addDataToBufferIn(compressedResult);
        ret = zlibCompressor1.decompress();
        std::string decompressedResult(zlibCompressor1.getBufferOut(), zlibCompressor1.getBufferOutLen());
        pbResult.ParseFromString(decompressedResult);
        uint64_t endTime = TimeUtility::currentTime();
        
        totalSize += compressedResult.size();

        parseTime = parseTime + endTime - startTime;
    }

    // HA3_LOG(ERROR, "zlib (%d)PB FormatResult performance:\n toTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%d B)", type,
    //         toTime / 1000, parseTime / 1000, hitNum, attributeNum, totalSize / 1000);
    printf("zlib (%d)PB FormatResult performance:\ntoTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%lu B)\n", type,
           toTime / 1000, parseTime / 1000, hitNum, attributeNum, totalSize / 1000);    
}

Hits* ParsePBFormatResultTest::createHits(uint32_t hitNum, uint32_t attributeNum) {
    Hits* hits = new Hits;
    hits->setNoSummary(true);
    hits->setIndependentQuery(true);
    for (uint32_t i = 0; i < hitNum; ++i) {
        HitPtr hit(new Hit);
//        std::string gidStr = "cluster0_1_2_3_" + StringUtil::toString(i);
        GlobalIdentifier gid(i, 3, 2, 0, 0, 1, 0);
        hit->setGlobalIdentifier(gid);
        hit->setHasPrimaryKeyFlag(true);
        for (uint32_t k = 0; k < attributeNum; ++k) {
            std::string attributeName = "attribute" + StringUtil::toString(k);
            hit->addAttribute(attributeName, AttributeItemPtr(new AttributeItemTyped<uint32_t>(i*k)));
        }
        hit->setSortValue((double)i);
        hits->addHit(hit);
    }
    return hits;
}

void ParsePBFormatResultTest::fillNewPbResult(PBResult pbResult, 
        PBResult &newPbResult)
{ 
    newPbResult.set_totaltime(pbResult.totaltime());
    *newPbResult.mutable_aggresults() = pbResult.aggresults();
    *newPbResult.mutable_errorresults() = pbResult.errorresults();
    newPbResult.set_tracer(pbResult.tracer());
    newPbResult.set_fromcache(pbResult.fromcache());

    PBMatchDocs* pbMatchDocs = newPbResult.mutable_matchdocs();
    fillNewPbResultVariableValues(pbResult, pbMatchDocs);
    fillNewPbResultAttributes(pbResult, pbMatchDocs);
    pbMatchDocs->set_nummatchdocs(pbResult.hits().numhits());
    pbMatchDocs->set_totalmatchdocs(pbResult.hits().totalhits());
    PBHits hits = pbResult.hits();

    PBSortValues* pbSortValues = pbMatchDocs->mutable_sortvalues();
    int32_t sortDimensionCount = hits.hit(0).sortvalues_size();
    pbSortValues->set_dimensioncount(sortDimensionCount);

    for (int i = 0; i < hits.hit().size(); ++i) {
        pbMatchDocs->add_clusterids(0);
        pbMatchDocs->add_hashids(hits.hit(i).hashid());
        pbMatchDocs->add_docids(hits.hit(i).docid());
        pbMatchDocs->add_fullindexversions(hits.hit(i).fullindexversion());
        pbMatchDocs->add_indexversions(hits.hit(i).indexversion());
        pbMatchDocs->add_pkhighers(hits.hit(i).pkhigher());
        pbMatchDocs->add_pklowers(hits.hit(i).pklower());
        pbMatchDocs->add_searcherips(hits.hit(i).searcherip());
        for (int j = 0; j < sortDimensionCount; j++) {
            pbSortValues->add_sortvalues(StringUtil::fromString<double>(hits.hit(i).sortvalues(j)));
        }            
    }
}

TEST_F(ParsePBFormatResultTest, testPBSerilizeAndDeserilizeSpeed) {
    cout << "################################start##################################" << endl;

    /* set pbResult by old.proto */
    PBResult pbResult;
    {
        fstream in("ha3/common/perftest/galaxy_pbdata", ios::in | ios::binary);
        if (!pbResult.ParseFromIstream(&in)) {
            cerr << "Parse Failed!" << endl;
            return;
        }
    }

    PBResult newPbResult;
    fillNewPbResult(pbResult, newPbResult);
    /* test latency with old.proto */
    {
        uint64_t startTime = TimeUtility::currentTime();
        for (int i = 0; i < 100; ++i) {
            string ss;
            pbResult.SerializeToString(&ss);
            PBResult deSerilizeResult;        
            if (!deSerilizeResult.ParseFromString(ss)) {
                cerr << "Parse Failed!" << endl;
            }
        }
        uint64_t endTime = TimeUtility::currentTime();
        cout << "Old proto Parse Time: " <<  (endTime - startTime) / 100 << endl;
    }

    cout << "##################Start##############################" << endl;
    /* test latency with new.proto */
    {
        fstream out("ha3/common/perftest/galaxy_new_pbdata", ios::out | ios::binary | ios::trunc);
        newPbResult.SerializeToOstream(&out);
        out.close();

        uint64_t startTime = TimeUtility::currentTime();
        for (int i = 0; i < 100; ++i) {
            string ss2;
            newPbResult.SerializeToString(&ss2);
            PBResult deSerilizeResult2;
            if (!deSerilizeResult2.ParseFromString(ss2)) {
                cerr << "Parse Failed!" << endl;
            }
        }
        uint64_t endTime = TimeUtility::currentTime();
        cout << "New proto Parse Time: " <<  (endTime - startTime) / 100 << endl;
    }
}

void ParsePBFormatResultTest::fillNewPbResultVariableValues(PBResult pbResult, 
        PBMatchDocs* pbMatchDocs) 
{
    for (int i = 0; i < pbResult.hits().hit(0).variablevalues().size(); ++i) {
        //valueType;
        string variableValueKey = pbResult.hits().hit(0).variablevalues(i).key();
        if (pbResult.hits().hit(0).variablevalues(i).int64value_size()) {
            PBInt64Attribute* pbInt64Attribute = pbMatchDocs->add_int64attrvalues();
            pbInt64Attribute->set_key(variableValueKey);
            pbInt64Attribute->set_type(VARIABLE_VALUE_TYPE);
            //for each hit
            for (int j = 0; j < pbResult.hits().hit_size(); j++) {
                int size = pbResult.hits().hit(j).variablevalues(i).int64value_size();
                pbInt64Attribute->add_offset(pbInt64Attribute->int64value_size());
                for (int k = 0; k < size; k++) {
                    int64_t value = pbResult.hits().hit(j).variablevalues(i).int64value(k);
                    pbInt64Attribute->add_int64value(value);
                }
            }
                
        } else if (pbResult.hits().hit(0).variablevalues(i).doublevalue_size()) {
            PBDoubleAttribute* pbDoubleAttribute = pbMatchDocs->add_doubleattrvalues();
            pbDoubleAttribute->set_key(variableValueKey);
            pbDoubleAttribute->set_type(VARIABLE_VALUE_TYPE);
            //for each hit
            for (int j = 0; j < pbResult.hits().hit_size(); j++) {
                int size = pbResult.hits().hit(j).variablevalues(i).doublevalue_size();
                pbDoubleAttribute->add_offset(pbDoubleAttribute->doublevalue_size());
                for (int k = 0; k < size; k++) {
                    double value = pbResult.hits().hit(j).variablevalues(i).doublevalue(k);
                    pbDoubleAttribute->add_doublevalue(value);
                }
            }

        } else if (pbResult.hits().hit(0).variablevalues(i).bytesvalue_size()){
            PBBytesAttribute* pbBytesAttribute = pbMatchDocs->add_bytesattrvalues();
            pbBytesAttribute->set_key(variableValueKey);
            pbBytesAttribute->set_type(VARIABLE_VALUE_TYPE);
            //for each hit
            for (int j = 0; j < pbResult.hits().hit_size(); j++) {
                int size = pbResult.hits().hit(j).variablevalues(i).bytesvalue_size();
                pbBytesAttribute->add_offset(pbBytesAttribute->bytesvalue_size());
                for (int k = 0; k < size; k++) {
                    string value = pbResult.hits().hit(j).variablevalues(i).bytesvalue(k);
                    pbBytesAttribute->add_bytesvalue(value);
                }
            }
        }
    } 
}

void ParsePBFormatResultTest::fillNewPbResultAttributes(PBResult pbResult, 
        PBMatchDocs* pbMatchDocs) 
{
    for (int i = 0; i < pbResult.hits().hit(0).attributes().size(); ++i) {
        string variableValueKey = pbResult.hits().hit(0).attributes(i).key();
        if (pbResult.hits().hit(0).attributes(i).int64value_size()) {
            PBInt64Attribute* pbInt64Attribute = pbMatchDocs->add_int64attrvalues();
            pbInt64Attribute->set_key(variableValueKey);
            pbInt64Attribute->set_type(ATTRIBUTE_TYPE);
            //for each hit
            for (int j = 0; j < pbResult.hits().hit_size(); j++) {
                int size = pbResult.hits().hit(j).attributes(i).int64value_size();
                pbInt64Attribute->add_offset(pbInt64Attribute->int64value_size());
                for (int k = 0; k < size; k++) {
                    int64_t value = pbResult.hits().hit(j).attributes(i).int64value(k);
                    pbInt64Attribute->add_int64value(value);
                }
            }
                
        } else if (pbResult.hits().hit(0).attributes(i).doublevalue_size()) {
            PBDoubleAttribute* pbDoubleAttribute = pbMatchDocs->add_doubleattrvalues();
            pbDoubleAttribute->set_key(variableValueKey);
            pbDoubleAttribute->set_type(ATTRIBUTE_TYPE);
            //for each hit
            for (int j = 0; j < pbResult.hits().hit_size(); j++) {
                int size = pbResult.hits().hit(j).attributes(i).doublevalue_size();
                pbDoubleAttribute->add_offset(pbDoubleAttribute->doublevalue_size());
                for (int k = 0; k < size; k++) {
                    double value = pbResult.hits().hit(j).attributes(i).doublevalue(k);
                    pbDoubleAttribute->add_doublevalue(value);
                }
            }

        } else if (pbResult.hits().hit(0).attributes(i).bytesvalue_size()){
            PBBytesAttribute* pbBytesAttribute = pbMatchDocs->add_bytesattrvalues();
            pbBytesAttribute->set_key(variableValueKey);
            pbBytesAttribute->set_type(ATTRIBUTE_TYPE);
            //for each hit
            for (int j = 0; j < pbResult.hits().hit_size(); j++) {
                int size = pbResult.hits().hit(j).attributes(i).bytesvalue_size();
                pbBytesAttribute->add_offset(pbBytesAttribute->bytesvalue_size());
                for (int k = 0; k < size; k++) {
                    string value = pbResult.hits().hit(j).attributes(i).bytesvalue(k);
                    pbBytesAttribute->add_bytesvalue(value);
                }
            }
        }
    }
}

END_HA3_NAMESPACE(common);

