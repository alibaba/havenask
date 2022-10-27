#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/HaCompressUtil.h>
#include <string.h>

using namespace std;
BEGIN_HA3_NAMESPACE(util);

class HaCompressUtilTest : public TESTBASE {
public:
    HaCompressUtilTest();
    ~HaCompressUtilTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestDifferentCompress(HaCompressType type);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, HaCompressUtilTest);


HaCompressUtilTest::HaCompressUtilTest() { 
}

HaCompressUtilTest::~HaCompressUtilTest() { 
}

void HaCompressUtilTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void HaCompressUtilTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(HaCompressUtilTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    innerTestDifferentCompress(Z_SPEED_COMPRESS);
    innerTestDifferentCompress(Z_DEFAULT_COMPRESS);
}
void HaCompressUtilTest::innerTestDifferentCompress(HaCompressType type)
{
    string result = "original string!!";
    string compressedResult;
    ASSERT_TRUE(HaCompressUtil::compress(result,
                    type, compressedResult));
    autil::ZlibCompressor compressor;
    compressor.addDataToBufferIn(compressedResult);
    ASSERT_TRUE(compressor.decompress());
    string decompressedString(compressor.getBufferOut(),
                                   compressor.getBufferOutLen());
    ASSERT_EQ(result, decompressedString);
}

TEST_F(HaCompressUtilTest, testNoCompress) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string result = "original string!!";
    string compressedResult;
    ASSERT_TRUE(!HaCompressUtil::compress(result,
                    NO_COMPRESS, compressedResult));
}

void checkCompressWithType(const string &original, HaCompressType type) {
    string compressed;
    string decompressed;
    ASSERT_TRUE(HaCompressUtil::compress(original, type, compressed));
    ASSERT_TRUE(HaCompressUtil::decompress(compressed, type, decompressed));
    ASSERT_EQ(original, decompressed);
}

TEST_F(HaCompressUtilTest, testDecompressNormal) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string original = "original string!!";
        checkCompressWithType(original, Z_SPEED_COMPRESS);
        checkCompressWithType(original, Z_DEFAULT_COMPRESS);
        checkCompressWithType(original, SNAPPY);
        checkCompressWithType(original, LZ4);
    }
    {
        string original = "aaaaaaaaaa";
        checkCompressWithType(original, Z_SPEED_COMPRESS);
        checkCompressWithType(original, Z_DEFAULT_COMPRESS);
        checkCompressWithType(original, SNAPPY);
        checkCompressWithType(original, LZ4);
    }
    {
        string original = "a";
        checkCompressWithType(original, Z_SPEED_COMPRESS);
        checkCompressWithType(original, Z_DEFAULT_COMPRESS);
        checkCompressWithType(original, SNAPPY);
        checkCompressWithType(original, LZ4);
    }
    {
        string original = "";
        checkCompressWithType(original, SNAPPY);
        checkCompressWithType(original, LZ4);
    }
}

TEST_F(HaCompressUtilTest, testLZ4DecompressHeader) {
    string original = "original string";
    string compressed;
    string decompressed;
    ASSERT_TRUE(HaCompressUtil::compress(original, LZ4,
                    compressed));
    int originalSize;
    memcpy(&originalSize, compressed.c_str(), sizeof(int));
    ASSERT_EQ(originalSize, original.length());
}

TEST_F(HaCompressUtilTest, testLZ4DecompressException) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string result = "ori";
    string decompressedResult;
    ASSERT_TRUE(!HaCompressUtil::decompress(result,
                    LZ4, decompressedResult));
}

END_HA3_NAMESPACE(util);

