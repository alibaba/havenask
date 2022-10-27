#ifndef __INDEXLIB_EQUIVALENTCOMPRESSWRITERTEST_H
#define __INDEXLIB_EQUIVALENTCOMPRESSWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"

IE_NAMESPACE_BEGIN(common);

class EquivalentCompressWriterTest : public INDEXLIB_TESTBASE {
public:
    class MockEquivalentCompressWriter : public EquivalentCompressWriterBase
    {
    public:
        MOCK_METHOD0(InitSlotBuffer, void());
        MOCK_METHOD0(FreeSlotBuffer, void());
        MOCK_METHOD0(FlushSlotBuffer, void());
    };

public:
    EquivalentCompressWriterTest();
    ~EquivalentCompressWriterTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestReset();
    void TestInit();
    void TestGetCompressLength();
    void TestGetUpperPowerNumber();
    void TestDumpBuffer();
    void TestDumpFile();
    void TestDump();
    void TestGetMaxDeltaInBuffer();
    void TestConvertToDeltaBuffer();
    void TestGetMinDeltaBufferSize();
    void TestFlushDeltaBuffer();
    void TestNeedStoreDelta();
    void TestAppendSlotItem();
    void TestFlushSlotBuffer();
    void TestCalculateDeltaBufferSize();
    void TestCalculateCompressLength();
    void TestCompressData();

    void TestSimpleBitCompress();
    void TestBitCompress();

private:
    void PrepareMockWriterForDump(MockEquivalentCompressWriter& mockWriter);
    void CheckDumpedFile(const std::string& filePath);

    template <typename T, int maxValueInTailSlot>
    void TestTypedBitCompress()
    {
        const size_t valueCount = 300;
        T value[valueCount] = {0};

        for (size_t i = 0; i < 64; i++)  // first slot data, 1 bit slot
        {
            value[0] = (T)(i % 2);
        }

        for (size_t i = 64; i < 128; i++) // second slot data, 2 bit slot
        {
            value[i] = (T)(i % 4);
        }

        for (size_t i = 128; i < 192; i++) // third slot data, 4 bit slot
        {
            value[i] = (T)(i % 16);
        }

        value[192] = 30;  // fourth 1 byte slot

        assert(maxValueInTailSlot <= 16);
        for (size_t i = 256; i < 300; i++) // fifth slot data
        {
            value[i] = (T)(i % maxValueInTailSlot);
        }
        
        size_t calculateLen = EquivalentCompressWriter<T>::CalculateCompressLength(
                value, valueCount, 64);

        EquivalentCompressWriter<T> writer;
        writer.Init(64);
        writer.CompressData(value, valueCount);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));
        ASSERT_EQ(compressSize, calculateLen);

        EquivalentCompressReader<T> reader(buffer);
        for (size_t i = 0; i < valueCount; ++i)
        {
            ASSERT_EQ(value[i], reader[i]);
        }
        delete[] buffer;   
    }

private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestReset);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestGetCompressLength);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestGetUpperPowerNumber);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestDumpBuffer);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestDumpFile);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestGetMaxDeltaInBuffer);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestConvertToDeltaBuffer);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestGetMinDeltaBufferSize);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestFlushDeltaBuffer);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestNeedStoreDelta);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestAppendSlotItem);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestFlushSlotBuffer);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestCalculateDeltaBufferSize);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestCalculateCompressLength);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestCompressData);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestSimpleBitCompress);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressWriterTest, TestBitCompress);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENTCOMPRESSWRITERTEST_H
