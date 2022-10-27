#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/group_varint.h"
#include <stdio.h>

using namespace std;

IE_NAMESPACE_BEGIN(common);


class GroupVarintTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(GroupVarintTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForGroupVarint()
    {
        uint32_t src[] = {3, 10, 20, 279, 3, 10, 20, 279};
        uint8_t dest[12] = {0};
        
        uint32_t len = GroupVarint::Compress(dest, 
                sizeof(dest)/sizeof(dest[0]), src, 
                sizeof(src)/sizeof(src[0]));
        INDEXLIB_TEST_EQUAL((uint32_t)12, len);
        
        uint32_t decodeDest[100];
        uint32_t decodeLen = GroupVarint::Decompress(decodeDest,
                sizeof(decodeDest)/sizeof(decodeDest[0]), dest,
                sizeof(src)/sizeof(src[0]));
        INDEXLIB_TEST_EQUAL((uint32_t)8, decodeLen);

        for (size_t i = 0; i < decodeLen; ++i) {
            INDEXLIB_TEST_EQUAL(src[i], decodeDest[i]);            
        }
    }

    void TestCaseForGroupVarintBlock() 
    {
        TestGroupVaintByNumber(4);
        TestGroupVaintByNumber(128);
        TestGroupVaintByNumber(130);
        TestGroupVaintByNumber(1280);
    }

    void TestCompressItem()
    {
        uint32_t src[4];
        src[0] = 1;
        src[1] = MAX_UINT8 + 3;
        src[2] = MAX_UINT16 + 9;
        src[3] = MAX_UINT24 + 125;
        uint8_t dest[11];
        uint32_t compressLen = GroupVarint::CompressItem(dest, src);
        INDEXLIB_TEST_EQUAL((uint32_t)11, compressLen);

        uint32_t decodedBuf[4];
        uint32_t retLen = GroupVarint::DecompressItem(decodedBuf, dest);
        INDEXLIB_TEST_EQUAL((uint32_t)11, retLen);
        for (size_t i = 0; i < 4; ++i)
        {
            INDEXLIB_TEST_EQUAL(src[i], decodedBuf[i]);
        }
    }

    void TestGroupVaintByNumber(uint32_t len) {
        uint32_t src[len];

        for(uint32_t i = 0; i < len; i++) {
            src[i] = i * 9;
        }
        TestGroupVarint(src, len);
    }
    
    void TestGroupVarint(uint32_t *src, uint32_t srcLen) {
        uint32_t bufferLen = srcLen << 3;
        uint8_t buffer[bufferLen];
        GroupVarint::Compress(buffer, bufferLen, src, srcLen);
        
        uint32_t destLen = srcLen;
        uint32_t dest[destLen];
        uint32_t decodeLen = GroupVarint::Decompress(dest,
                destLen, buffer, srcLen);
        INDEXLIB_TEST_EQUAL(srcLen, decodeLen);

        for (size_t i = 0; i < decodeLen; ++i) {
            INDEXLIB_TEST_EQUAL(src[i], dest[i]);            
        }
    }


private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(common, GroupVarintTest);

INDEXLIB_UNIT_TEST_CASE(GroupVarintTest, TestCaseForGroupVarint);
INDEXLIB_UNIT_TEST_CASE(GroupVarintTest, TestCaseForGroupVarintBlock);
INDEXLIB_UNIT_TEST_CASE(GroupVarintTest, TestCompressItem);

IE_NAMESPACE_END(common);
