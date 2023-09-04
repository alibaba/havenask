#include "swift/util/CompressUtil.h"

#include <iosfwd>
#include <string>

#include "autil/ZlibCompressor.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace util {

class CompressUtilTest : public TESTBASE {
public:
    void innerTestDifferentCompress(SwiftCompressType type);

protected:
};

TEST_F(CompressUtilTest, testSimpleProcess) {
    innerTestDifferentCompress(SWIFT_Z_SPEED_COMPRESS);
    innerTestDifferentCompress(SWIFT_Z_DEFAULT_COMPRESS);
}
void CompressUtilTest::innerTestDifferentCompress(SwiftCompressType type) {
    string result = "original string!!";
    string compressedResult;
    ASSERT_TRUE(CompressUtil::compress(result, type, compressedResult));
    autil::ZlibCompressor compressor;
    compressor.addDataToBufferIn(compressedResult);
    ASSERT_TRUE(compressor.decompress());
    string decompressedString(compressor.getBufferOut(), compressor.getBufferOutLen());
    ASSERT_EQ(result, decompressedString);
}

TEST_F(CompressUtilTest, testNoCompress) {
    string result = "original string!!";
    string compressedResult;
    ASSERT_TRUE(!CompressUtil::compress(result, SWIFT_NO_COMPRESS, compressedResult));
}

void checkCompressWithType(const string &original, SwiftCompressType type) {
    string compressed;
    string decompressed;
    ASSERT_TRUE(CompressUtil::compress(original, type, compressed));
    ASSERT_TRUE(CompressUtil::decompress(compressed, type, decompressed));
    ASSERT_EQ(original, decompressed);
}

TEST_F(CompressUtilTest, testDecompressNormal) {
    {
        string original = "original string!!";
        checkCompressWithType(original, SWIFT_Z_SPEED_COMPRESS);
        checkCompressWithType(original, SWIFT_Z_DEFAULT_COMPRESS);
        checkCompressWithType(original, SWIFT_Z_BEST_COMPRESS);
        checkCompressWithType(original, SWIFT_SNAPPY);
        // checkCompressWithType(original, SWIFT_LZ4);
    }
    {
        string original = "aaaaaaaaaa";
        checkCompressWithType(original, SWIFT_Z_SPEED_COMPRESS);
        checkCompressWithType(original, SWIFT_Z_DEFAULT_COMPRESS);
        checkCompressWithType(original, SWIFT_Z_BEST_COMPRESS);
        checkCompressWithType(original, SWIFT_SNAPPY);
        // checkCompressWithType(original, SWIFT_LZ4);
    }
    {
        string original = "a";
        checkCompressWithType(original, SWIFT_Z_SPEED_COMPRESS);
        checkCompressWithType(original, SWIFT_Z_DEFAULT_COMPRESS);
        checkCompressWithType(original, SWIFT_Z_BEST_COMPRESS);
        checkCompressWithType(original, SWIFT_SNAPPY);
        // checkCompressWithType(original, SWIFT_LZ4);
    }
    {
        string original = "";
        checkCompressWithType(original, SWIFT_SNAPPY);
        // checkCompressWithType(original, SWIFT_LZ4);
    }
}

TEST_F(CompressUtilTest, testLZ4DecompressHeader) {
    // string original = "original string";
    // string compressed;
    // string decompressed;
    // ASSERT_TRUE(CompressUtil::compress(original, SWIFT_LZ4, compressed));
    // int originalSize;
    // memcpy(&originalSize, compressed.c_str(), sizeof(int));
    // ASSERT_EQ(originalSize, original.length());
}

TEST_F(CompressUtilTest, testLZ4DecompressException) {
    // string result = "ori";
    // string decompressedResult;
    // ASSERT_TRUE(!CompressUtil::decompress(result, SWIFT_LZ4, decompressedResult));
}

} // namespace util
} // namespace swift
