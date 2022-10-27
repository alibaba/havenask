#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/attribute/multi_string_attribute_convertor.h"
#include <autil/StringUtil.h>
#include <autil/MultiValueType.h>
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);

class MultiStringAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    void SetUp()
    {
    }

    void TearDown()
    {
    }

    void TestCaseForSimple()
    {
        vector<string> strVec;
        for (size_t i = 0; i < 10000; ++i)
        {
            string value = "attribute";
            value += autil::StringUtil::toString(i);
            strVec.push_back(value);
        }
        TestEncode(strVec, false, false);
        TestEncode(strVec, true, false);
        TestEncode(strVec, false, true);
        TestEncode(strVec, true, true);
    }

private:
    void TestEncode(vector<string>& strVec, bool needHash, bool isBinary)
    {
        string input;
        string expectedData;
        uint32_t expectedCount = 0;
        uint32_t offset = 0;
        vector<uint32_t> offsetVec;

        for (size_t i = 0; i < strVec.size(); ++i)
        {
            input += strVec[i];
            if (i < strVec.size() - 1)
            {
                input += MULTI_VALUE_SEPARATOR;
            }

            size_t encodeCountLen = 
                VarNumAttributeFormatter::GetEncodedCountLength(strVec[i].size());
            offsetVec.push_back(offset);
            offset += (strVec[i].size() + encodeCountLen);
            AppendCount(expectedData, strVec[i].size());
            expectedData += strVec[i];
            ++expectedCount;
        }

        if (isBinary)
        {
            autil::DataBuffer dataBuffer;
            dataBuffer.write(strVec);
            input = string(dataBuffer.getData(), dataBuffer.getDataLen());
        }
        MultiStringAttributeConvertor convertor(needHash, "", isBinary);
        string result = convertor.Encode(input);
        // test EncodeFromAttrValueMeta
        ConstString encodedStr1(result.data(), result.size());
        AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
        ConstString encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &mPool);
        ASSERT_EQ(encodedStr1, encodedStr2);
        
        uint64_t expectedHashKey = (uint64_t)-1;
        if (needHash)
        {
            expectedHashKey = util::HashString::Hash(input.data(), input.size());
        }

        // check hashKey
        const char* ptr = result.c_str();
        uint64_t hashKey = *(uint64_t*)ptr;
        INDEXLIB_TEST_EQUAL(expectedHashKey, hashKey);
        ptr += sizeof(uint64_t);

        // check count
        size_t encodeCountLen = 0;
        uint32_t count = 
            VarNumAttributeFormatter::DecodeCount(ptr, encodeCountLen);
        INDEXLIB_TEST_EQUAL(expectedCount, count);
        ptr += encodeCountLen;

        uint8_t expectOffsetLen = VarNumAttributeFormatter::GetOffsetItemLength(
                *offsetVec.rbegin());
        ASSERT_EQ(expectOffsetLen, *(uint8_t*)ptr);
        ptr++;

        // check offset
        for(uint16_t i = 0; i < count; ++i)
        {
            uint32_t offset = VarNumAttributeFormatter::GetOffset(
                    ptr, expectOffsetLen, i);
            INDEXLIB_TEST_EQUAL(offsetVec[i], offset);
        }
        ptr += (expectOffsetLen * count);

        const char* endPtr = result.c_str() + result.size();
        // check string data
        INDEXLIB_TEST_EQUAL(expectedData,
                            string(ptr, endPtr - ptr));
    }

    void AppendCount(std::string& str, uint32_t count)
    {
        char buffer[4];
        size_t encodeLen = VarNumAttributeFormatter::EncodeCount(count, buffer, 4);
        str.append(buffer, encodeLen);
    }
private:
    autil::mem_pool::Pool mPool;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(common, MultiStringAttributeConvertorTest);

INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeConvertorTest, TestCaseForSimple);
    

IE_NAMESPACE_END(common);
