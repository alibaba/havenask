#ifndef __INDEXLIB_COMPRESSOFFSETREADERTEST_H
#define __INDEXLIB_COMPRESSOFFSETREADERTEST_H

#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"
#include "indexlib/index/normal/attribute/accessor/compress_offset_reader.h"

IE_NAMESPACE_BEGIN(index);

class CompressOffsetReaderTest : public INDEXLIB_TESTBASE
{
public:
    CompressOffsetReaderTest();
    ~CompressOffsetReaderTest();

    DECLARE_CLASS_NAME(CompressOffsetReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInit();
    void TestSetOffset();

private:
    template <typename T>
    void InnerTestInit(const std::string& offsetStr, 
                       uint32_t docCount, bool invalidTail,
                       bool hasExpandSlice, bool expectException)
    {
        std::vector<uint64_t> offsetVec;
        autil::StringUtil::fromString(offsetStr, offsetVec, ",");

        uint8_t* buffer = NULL;
        size_t bufferLen;
        PrepareCompressedOffsetBuffer<T>(offsetVec, buffer, bufferLen);

        file_system::SliceFileReaderPtr sliceFileReader;
        if (hasExpandSlice)
        {
            sliceFileReader.reset(new file_system::SliceFileReader(
                            file_system::SliceFileNodePtr()));
        }

        if (invalidTail)
        {
            uint32_t* magicTail = (uint32_t*)(buffer + bufferLen - 4);
            *magicTail = 0;
        }

        CompressOffsetReader offsetReader;
        if (expectException)
        {
            ASSERT_ANY_THROW(offsetReader.Init(buffer, bufferLen, docCount, sliceFileReader));
        }
        else
        {
            ASSERT_NO_THROW(offsetReader.Init(buffer, bufferLen, docCount, sliceFileReader));
            bool isU32Offset = (sizeof(T) == sizeof(uint32_t));
            ASSERT_EQ(isU32Offset, offsetReader.IsU32Offset());
        }
        delete[] buffer;
    }

private:
    template <typename T>
    void PrepareCompressedOffsetBuffer(
            const std::vector<uint64_t>& offsets,
            uint8_t*& buffer, size_t& bufferLen)
    {
        common::EquivalentCompressWriter<T> writer;
        writer.Init(64);
        for (size_t i = 0; i < offsets.size(); i++)
        {
            T value = (T)offsets[i];
            writer.CompressData(&value, 1);
        }

        bufferLen = writer.GetCompressLength() + sizeof(uint32_t);
        buffer = new uint8_t[bufferLen];
        writer.DumpBuffer(buffer, bufferLen);
        uint32_t* magic = (uint32_t*)(buffer + bufferLen - 4);

        if (sizeof(T) == sizeof(uint32_t))
        {
            *magic = UINT32_OFFSET_TAIL_MAGIC;
        }
        else
        {
            assert(sizeof(T) == sizeof(uint64_t));
            *magic = UINT64_OFFSET_TAIL_MAGIC;
        }
    }

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressOffsetReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CompressOffsetReaderTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(CompressOffsetReaderTest, TestSetOffset);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMPRESSOFFSETREADERTEST_H
