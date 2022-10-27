#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hits.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/common/XMLResultFormatter.h>
#include <autil/TimeUtility.h>
#include <mxml.h>
#include <autil/ZlibCompressor.h>

BEGIN_HA3_NAMESPACE(common);

class ParseXMLFormatResultTest : public TESTBASE {
public:
    ParseXMLFormatResultTest();
    ~ParseXMLFormatResultTest();
public:
    void setUp();
    void tearDown();
protected:
    Hits* createHits(uint32_t hitNum, uint32_t attributeNum);
    void testParseResult(uint32_t hitNum, uint32_t attrbiuteNum);
    void testParseResultFromBinary(uint32_t hitNum, uint32_t attributeNum);
    void testParseResultFromBinaryWithZlib(uint32_t hitNum, uint32_t attributeNum, int type);
    void testParseResultWithZlib(uint32_t hitNum, uint32_t attributeNum, int type);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ParseXMLFormatResultTest);

using namespace autil;


ParseXMLFormatResultTest::ParseXMLFormatResultTest() { 
}

ParseXMLFormatResultTest::~ParseXMLFormatResultTest() { 
}

void ParseXMLFormatResultTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ParseXMLFormatResultTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ParseXMLFormatResultTest, testSimpleProcess) {
    testParseResult(10, 2);
    testParseResult(10, 5);
    testParseResult(10, 10);

    testParseResult(1000, 2);
    testParseResult(1000, 5);
    testParseResult(1000, 10);
}
TEST_F(ParseXMLFormatResultTest, testParseFromBinary)
{
    testParseResultFromBinary(10, 2);
    testParseResultFromBinary(10, 5);
    testParseResultFromBinary(10, 10);

    testParseResultFromBinary(1000, 2);
    testParseResultFromBinary(1000, 5);
    testParseResultFromBinary(1000, 10);
}

TEST_F(ParseXMLFormatResultTest, testParseWithZlibDefault)
{
    testParseResultWithZlib(10, 2, Z_DEFAULT_COMPRESSION);
    testParseResultWithZlib(10, 5, Z_DEFAULT_COMPRESSION);
    testParseResultWithZlib(10, 10, Z_DEFAULT_COMPRESSION);

    testParseResultWithZlib(1000, 2, Z_DEFAULT_COMPRESSION);
    testParseResultWithZlib(1000, 5, Z_DEFAULT_COMPRESSION);
    testParseResultWithZlib(1000, 10, Z_DEFAULT_COMPRESSION);
}

TEST_F(ParseXMLFormatResultTest, testParseWithZlibSpeed)
{
    testParseResultWithZlib(10, 2, Z_BEST_SPEED);
    testParseResultWithZlib(10, 5, Z_BEST_SPEED);
    testParseResultWithZlib(10, 10, Z_BEST_SPEED);

    testParseResultWithZlib(1000, 2, Z_BEST_SPEED);
    testParseResultWithZlib(1000, 5, Z_BEST_SPEED);
    testParseResultWithZlib(1000, 10, Z_BEST_SPEED);
}

TEST_F(ParseXMLFormatResultTest, testParseFromBinaryWithZipDefault)
{
    testParseResultFromBinaryWithZlib(10, 2, Z_DEFAULT_COMPRESSION);
    testParseResultFromBinaryWithZlib(10, 5, Z_DEFAULT_COMPRESSION);
    testParseResultFromBinaryWithZlib(10, 10, Z_DEFAULT_COMPRESSION);

    testParseResultFromBinaryWithZlib(1000, 2, Z_DEFAULT_COMPRESSION);
    testParseResultFromBinaryWithZlib(1000, 5, Z_DEFAULT_COMPRESSION);
    testParseResultFromBinaryWithZlib(1000, 10, Z_DEFAULT_COMPRESSION);
}

TEST_F(ParseXMLFormatResultTest, testParseFromBinaryWithZipSpeed)
{
    testParseResultFromBinaryWithZlib(10, 2, Z_BEST_SPEED);
    testParseResultFromBinaryWithZlib(10, 5, Z_BEST_SPEED);
    testParseResultFromBinaryWithZlib(10, 10, Z_BEST_SPEED);

    testParseResultFromBinaryWithZlib(1000, 2, Z_BEST_SPEED);
    testParseResultFromBinaryWithZlib(1000, 5, Z_BEST_SPEED);
    testParseResultFromBinaryWithZlib(1000, 10, Z_BEST_SPEED);
}

void ParseXMLFormatResultTest::testParseResultFromBinary(uint32_t hitNum, uint32_t attributeNum) { 
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t totoalTime = 0;
    uint32_t totalSize = 0;
    uint64_t toTime = 0;

    for (uint32_t i = 0; i < 1000; ++i) {
        util::DataBuffer dataBuffer;
        Hits* hits = createHits(hitNum, attributeNum);
        ResultPtr result(new Result);
        result->setHits(hits);
        uint64_t toStartTime = TimeUtility::currentTime();
        result->serialize(dataBuffer);
        uint64_t toEndTime = TimeUtility::currentTime();
        toTime = toTime + toEndTime - toStartTime;

        totalSize += dataBuffer.getDataLen();
        ResultPtr result2(new Result);
        uint64_t startTime = TimeUtility::currentTime();    
        result2->deserialize(dataBuffer);
        uint64_t endTime = TimeUtility::currentTime();
        totoalTime = totoalTime + endTime - startTime;
    }
    
    HA3_LOG(ERROR, "ParseFromBinary performance:\n toTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%d B)", 
            toTime / 1000, totoalTime / 1000, hitNum, attributeNum, totalSize / 1000);
}

void ParseXMLFormatResultTest::testParseResultFromBinaryWithZlib(uint32_t hitNum, uint32_t attributeNum, int type) { 
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t totoalTime = 0;
    uint32_t totalSize = 0;
    uint64_t toTime = 0;

    for (uint32_t i = 0; i < 1000; ++i) {
        util::DataBuffer dataBuffer;
        ZlibCompressor zlibCompressor(type);
        Hits* hits = createHits(hitNum, attributeNum);
        ResultPtr result(new Result);
        result->setHits(hits);
        uint64_t toStartTime = TimeUtility::currentTime();
        result->serialize(dataBuffer);
        zlibCompressor.addDataToBufferIn(dataBuffer.getData(), dataBuffer.getDataLen());
        bool ret = zlibCompressor.compress();
        std::string compressedResult(zlibCompressor.getBufferOut(), zlibCompressor.getBufferOutLen());

        uint64_t toEndTime = TimeUtility::currentTime();
        toTime = toTime + toEndTime - toStartTime;

        totalSize += compressedResult.size();

        ResultPtr result2(new Result);
        uint64_t startTime = TimeUtility::currentTime();    
        ZlibCompressor zlibCompressor1(type);
        zlibCompressor1.addDataToBufferIn(compressedResult);
        ret = zlibCompressor1.decompress();
        std::string decompressedResult(zlibCompressor1.getBufferOut(), zlibCompressor1.getBufferOutLen());
        util::DataBuffer dataBuffer1;
        dataBuffer1.writeBytes(decompressedResult.c_str(), decompressedResult.length());
        result2->deserialize(dataBuffer1);
        uint64_t endTime = TimeUtility::currentTime();
        totoalTime = totoalTime + endTime - startTime;
    }
    
    HA3_LOG(ERROR, "ParseFromBinary(%d) performance:\n toTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%d B)", type,
            toTime / 1000, totoalTime / 1000, hitNum, attributeNum, totalSize / 1000);
}

void ParseXMLFormatResultTest::testParseResult(uint32_t hitNum, uint32_t attributeNum) { 
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t totoalTime = 0;
    uint64_t toTime = 0;
    uint32_t totalSize = 0;

    for (uint32_t i = 0; i < 1000; ++i) {
        Hits* hits = createHits(hitNum, attributeNum);
        ResultPtr result(new Result);
        result->setHits(hits);
        uint64_t toStartTime = TimeUtility::currentTime();
        std::string xmlFormatResult = XMLResultFormatter::xmlFormatResult(result);
        uint64_t toEndTime = TimeUtility::currentTime();
        toTime = toTime + toEndTime - toStartTime;
        totalSize += xmlFormatResult.size();

        uint64_t startTime = TimeUtility::currentTime();    
        mxml_node_t* XMLRoot = mxmlLoadString(NULL, xmlFormatResult.c_str(), MXML_NO_CALLBACK);
        uint64_t endTime = TimeUtility::currentTime();
        mxmlDelete(XMLRoot);

        totoalTime = totoalTime + endTime - startTime;
    }
    
    HA3_LOG(ERROR, "ParseXMLFormatResult performance:\n toTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%d B)", 
            toTime / 1000, totoalTime / 1000, hitNum, attributeNum, totalSize / 1000);
}

void ParseXMLFormatResultTest::testParseResultWithZlib(uint32_t hitNum, uint32_t attributeNum, int type) { 
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t totoalTime = 0;
    uint64_t toTime = 0;
    uint32_t totalSize = 0;

    for (uint32_t i = 0; i < 1000; ++i) {
        ZlibCompressor zlibCompressor(type);
        Hits* hits = createHits(hitNum, attributeNum);
        ResultPtr result(new Result);
        result->setHits(hits);
        uint64_t toStartTime = TimeUtility::currentTime();
        std::string xmlFormatResult = XMLResultFormatter::xmlFormatResult(result);
        zlibCompressor.addDataToBufferIn(xmlFormatResult);

        bool ret = zlibCompressor.compress();
        std::string compressedResult(zlibCompressor.getBufferOut(), zlibCompressor.getBufferOutLen());
 
        uint64_t toEndTime = TimeUtility::currentTime();
        toTime = toTime + toEndTime - toStartTime;
        
        totalSize += compressedResult.size();
        ZlibCompressor zlibCompressor1(type);
        uint64_t startTime = TimeUtility::currentTime();    
        zlibCompressor1.addDataToBufferIn(compressedResult);
        ret = zlibCompressor1.decompress();
        std::string decompressedResult(zlibCompressor1.getBufferOut(), zlibCompressor1.getBufferOutLen());
        mxml_node_t* XMLRoot = mxmlLoadString(NULL, decompressedResult.c_str(), MXML_NO_CALLBACK);
        uint64_t endTime = TimeUtility::currentTime();
        mxmlDelete(XMLRoot);

        totoalTime = totoalTime + endTime - startTime;
    }
    
    HA3_LOG(ERROR, "zlib(%d) performance:\n toTime(%lu us), parseTime:(%lu us), hitCount (%d), attributeNum(%d), averageSize(%d B)", type,
            toTime / 1000, totoalTime / 1000, hitNum, attributeNum, totalSize / 1000);
}

Hits* ParseXMLFormatResultTest::createHits(uint32_t hitNum, uint32_t attributeNum) {
    Hits* hits = new Hits;
    hits->setNoSummary(true);
    for (uint32_t i = 0; i < hitNum; ++i) {
        HitPtr hit(new Hit);
        std::string gidStr = "cluster0_1_2_3_" + StringUtil::toString(i);
        hit->fromGid(gidStr);
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

END_HA3_NAMESPACE(common);

