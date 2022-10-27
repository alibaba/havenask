#include "indexlib/common/numeric_compress/test/equivalent_compress_writer_unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace std;
using testing::Return;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, EquivalentCompressWriterTest);

EquivalentCompressWriterTest::EquivalentCompressWriterTest()
{
}

EquivalentCompressWriterTest::~EquivalentCompressWriterTest()
{
}

void EquivalentCompressWriterTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void EquivalentCompressWriterTest::CaseTearDown()
{
}

void EquivalentCompressWriterTest::TestReset()
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FreeSlotBuffer());
    
    writer.Reset();
    ASSERT_EQ((uint32_t)0, writer.mSlotItemPowerNum);
    ASSERT_EQ((uint32_t)0, writer.mSlotItemCount);
    ASSERT_EQ((uint32_t)0, writer.mItemCount);
    ASSERT_EQ((uint32_t)0, writer.mDataArrayCursor);
    ASSERT_TRUE(writer.mIndexArray == NULL);
    ASSERT_TRUE(writer.mDataArray == NULL);
}

void EquivalentCompressWriterTest::TestInit()
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FreeSlotBuffer()).Times(2);
    EXPECT_CALL(writer, InitSlotBuffer()).Times(2);
    EXPECT_CALL(writer, FlushSlotBuffer()).Times(0);

    writer.Init(7);
    ASSERT_EQ((uint32_t)6, writer.mSlotItemPowerNum);
    ASSERT_EQ((uint32_t)64, writer.mSlotItemCount);
    ASSERT_EQ((uint32_t)0, writer.mItemCount);
    ASSERT_EQ((uint32_t)0, writer.mDataArrayCursor);
    ASSERT_TRUE(writer.mIndexArray != NULL);
    ASSERT_TRUE(writer.mDataArray != NULL);

    writer.mItemCount = 8;
    writer.mDataArrayCursor = 18;

    writer.Init(255);
    ASSERT_EQ((uint32_t)8, writer.mSlotItemPowerNum);
    ASSERT_EQ((uint32_t)256, writer.mSlotItemCount);
    ASSERT_EQ((uint32_t)0, writer.mItemCount);
    ASSERT_EQ((uint32_t)0, writer.mDataArrayCursor);
    ASSERT_TRUE(writer.mIndexArray != NULL);
    ASSERT_TRUE(writer.mDataArray != NULL);
}

void EquivalentCompressWriterTest::TestGetCompressLength()
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer()).Times(2);

    {
        writer.mItemCount = 0;
        ASSERT_EQ((size_t)8, writer.GetCompressLength());
    }

    {
        writer.mItemCount = 127;
        writer.mSlotItemCount = 32;
        writer.mDataArrayCursor = 10;
        // 8 + 8 * 4 + 10
        ASSERT_EQ((size_t)50, writer.GetCompressLength());
    }
}

void EquivalentCompressWriterTest::TestGetUpperPowerNumber()
{
    MockEquivalentCompressWriter writer;
    ASSERT_EQ((uint32_t)6, writer.GetUpperPowerNumber(3));
    ASSERT_EQ((uint32_t)6, writer.GetUpperPowerNumber(8));
    ASSERT_EQ((uint32_t)7, writer.GetUpperPowerNumber(127));
    ASSERT_EQ((uint32_t)10, writer.GetUpperPowerNumber(1024));
    ASSERT_EQ((uint32_t)11, writer.GetUpperPowerNumber(1025));
}

void EquivalentCompressWriterTest::TestDumpBuffer()
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer())
        .WillRepeatedly(Return());
    PrepareMockWriterForDump(writer);

    uint8_t buffer[30];
    char* base = (char*)buffer;
    ASSERT_EQ(size_t(24),
              writer.DumpBuffer(buffer, sizeof(buffer)));

    ASSERT_EQ(*(uint32_t*)base, (uint32_t)1);
    ASSERT_EQ(*(uint32_t*)(base + 4), (uint32_t)3);
    ASSERT_EQ(*(uint64_t*)(base + 8), (uint64_t)1);
    ASSERT_EQ(*(uint64_t*)(base + 16), (uint64_t)1);
}

void EquivalentCompressWriterTest::TestDumpFile()
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer())
        .WillRepeatedly(Return());
    PrepareMockWriterForDump(writer);

    string filePath = storage::FileSystemWrapper::JoinPath(mRootDir, 
            "equivalent_compress_writer_test_dump_file");
    BufferedFileWriterPtr fileWriter(new BufferedFileWriter);
    fileWriter->Open(filePath);
    writer.Dump(fileWriter);
    fileWriter->Close();

    CheckDumpedFile(filePath);
}

void EquivalentCompressWriterTest::TestDump()
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer())
        .WillRepeatedly(Return());
    PrepareMockWriterForDump(writer);

    file_system::FileWriterPtr fileWriter = 
        GET_PARTITION_DIRECTORY()->CreateFileWriter(
                "equivalent_compress_writer_test_dump_file");

    string filePath = fileWriter->GetPath();
    writer.Dump(fileWriter);
    fileWriter->Close();
    CheckDumpedFile(filePath);
}

void EquivalentCompressWriterTest::TestGetMaxDeltaInBuffer()
{
    uint32_t minValue = 0;
    uint32_t maxDelta = 0;
    {
        uint32_t value[] = {17, 2, 3, 9, 17};
        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(
                value, 1, minValue);
        ASSERT_EQ((uint32_t)0, maxDelta);
        ASSERT_EQ((uint32_t)17, minValue);

        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(
                value, 4, minValue);
        ASSERT_EQ((uint32_t)15, maxDelta);
        ASSERT_EQ((uint32_t)2, minValue);

        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(
                value, 5, minValue);
        ASSERT_EQ((uint32_t)15, maxDelta);
        ASSERT_EQ((uint32_t)2, minValue);

    }

    {
        uint32_t value[] = {1, 1, 1, 1, 1};
        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(
                value, 5, minValue);
        ASSERT_EQ((uint32_t)0, maxDelta);
        ASSERT_EQ((uint32_t)1, minValue);
    }
}

void EquivalentCompressWriterTest::TestConvertToDeltaBuffer()
{
    uint32_t value[] = {17, 2, 3, 9, 17};
    uint32_t deltaValue[] = {15, 0, 1, 7, 15};

    EquivalentCompressWriter<uint32_t>::ConvertToDeltaBuffer(
            value, 5, 2);

    for (size_t i = 0; i < 5; ++i)
    {
        ASSERT_EQ(deltaValue[i], value[i]);
    }
}

void EquivalentCompressWriterTest::TestGetMinDeltaBufferSize()
{
    ASSERT_EQ((size_t)1, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(1, 8));
    ASSERT_EQ((size_t)2, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(2, 8));
    ASSERT_EQ((size_t)2, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(3, 8));
    ASSERT_EQ((size_t)4, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(4, 8));
    ASSERT_EQ((size_t)4, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(15, 8));
    ASSERT_EQ((size_t)8, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(16, 8));
    ASSERT_EQ((size_t)8, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(255, 8));
    ASSERT_EQ((size_t)16, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(256, 8));
    ASSERT_EQ((size_t)16, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(65535, 8));
    ASSERT_EQ((size_t)32, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(65536, 8));
    ASSERT_EQ((size_t)32, EquivalentCompressWriter<uint64_t>::GetMinDeltaBufferSize((uint64_t)((uint32_t)-1), 8));
    ASSERT_EQ((size_t)64, EquivalentCompressWriter<uint64_t>::GetMinDeltaBufferSize((uint64_t)((uint32_t)-1) + 1, 8));

    // align to byte when bit compress
    ASSERT_EQ((size_t)1, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(1, 1));
    ASSERT_EQ((size_t)2, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(1, 9));
    ASSERT_EQ((size_t)2, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(2, 7));
    ASSERT_EQ((size_t)3, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(2, 9));
    ASSERT_EQ((size_t)4, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(15, 7));
    ASSERT_EQ((size_t)4, EquivalentCompressWriter<uint32_t>::GetMinDeltaBufferSize(15, 8));
}

void EquivalentCompressWriterTest::TestFlushDeltaBuffer()
{
    {
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);

        uint32_t deltaBuffer[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint32_t expectDelta[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint32_t minValue = 2;
        
        size_t size = writer.FlushDeltaBuffer(deltaBuffer, 8, minValue, SIT_DELTA_UINT8);
        
        uint32_t value;
        writer.mDataArray->GetTypedValue<uint32_t>(0, value);
        ASSERT_EQ(minValue, value);

        uint8_t deltaValue;
        for (uint32_t i = 0; i < 8; ++i)
        {
            writer.mDataArray->GetTypedValue<uint8_t>(4 + i, deltaValue);
            ASSERT_EQ(expectDelta[i], (uint32_t)deltaValue);
        }

        ASSERT_EQ((size_t)12, size);
    }

    {
        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);

        uint64_t deltaBuffer[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint64_t expectDelta[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint64_t minValue = 2;
        uint64_t deltaPowerNum = 1;
        
        size_t size = writer.FlushDeltaBuffer(deltaBuffer, 8, minValue, SIT_DELTA_UINT16);
        
        uint64_t value;
        writer.mDataArray->GetTypedValue<uint64_t>(0, value);
        ASSERT_EQ(minValue, value);
        writer.mDataArray->GetTypedValue<uint64_t>(8, value);
        ASSERT_EQ(deltaPowerNum, value);

        uint16_t deltaValue;
        for (uint32_t i = 0; i < 8; ++i)
        {
            writer.mDataArray->GetTypedValue<uint16_t>(16 + sizeof(uint16_t)*i, deltaValue);
            ASSERT_EQ(expectDelta[i], (uint64_t)deltaValue);
        }
        ASSERT_EQ((size_t)32, size);
    }

    {
        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);

        uint64_t deltaBuffer[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint64_t expectDelta[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint64_t minValue = 2;
        uint64_t deltaPowerNum = 1;
        
        size_t size = writer.FlushDeltaBuffer(deltaBuffer, 8, minValue, SIT_DELTA_UINT16);
        
        uint64_t value;
        writer.mDataArray->GetTypedValue<uint64_t>(0, value);
        ASSERT_EQ(minValue, value);
        writer.mDataArray->GetTypedValue<uint64_t>(8, value);
        ASSERT_EQ(deltaPowerNum, value);

        uint16_t deltaValue;
        for (uint32_t i = 0; i < 8; ++i)
        {
            writer.mDataArray->GetTypedValue<uint16_t>(16 + sizeof(uint16_t)*i, deltaValue);
            ASSERT_EQ(expectDelta[i], (uint64_t)deltaValue);
        }
        ASSERT_EQ((size_t)32, size);
    }
}

void EquivalentCompressWriterTest::TestNeedStoreDelta()
{
    ASSERT_TRUE(EquivalentCompressWriter<uint32_t>::NeedStoreDelta(1, 3));
    ASSERT_FALSE(EquivalentCompressWriter<uint32_t>::NeedStoreDelta(0, 3));

    ASSERT_TRUE(EquivalentCompressWriter<uint64_t>::NeedStoreDelta(1, 3));
    ASSERT_TRUE(EquivalentCompressWriter<uint64_t>::NeedStoreDelta(0, std::numeric_limits<int64_t>::max()+1ul));
    ASSERT_FALSE(EquivalentCompressWriter<int64_t>::NeedStoreDelta(0, std::numeric_limits<int64_t>::max()));
    ASSERT_FALSE(EquivalentCompressWriter<uint64_t>::NeedStoreDelta(0, std::numeric_limits<int64_t>::max()));
}

void EquivalentCompressWriterTest::TestAppendSlotItem()
{
    {
        union SlotItemUnion
        {
            SlotItem slotItem;
            uint64_t slotValue;
        };
        SlotItemUnion slotUnion;

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.AppendSlotItem(SIT_EQUAL, 34);
        slotUnion.slotValue = (*writer.mIndexArray)[0];

        ASSERT_EQ(SIT_EQUAL, slotUnion.slotItem.slotType);
        ASSERT_EQ((uint64_t)34, slotUnion.slotItem.value);

        writer.mItemCount = 64;
        writer.AppendSlotItem(SIT_DELTA_UINT8, 321);
        slotUnion.slotValue = (*writer.mIndexArray)[1];

        ASSERT_EQ(SIT_DELTA_UINT8, slotUnion.slotItem.slotType);
        ASSERT_EQ((uint64_t)321, slotUnion.slotItem.value);
    }

    {
        union SlotItemUnion
        {
            LongSlotItem slotItem;
            uint64_t slotValue;
        };
        SlotItemUnion slotUnion;

        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);
        writer.AppendSlotItem(SIT_EQUAL, 34);
        slotUnion.slotValue = (*writer.mIndexArray)[0];
        ASSERT_EQ((uint64_t)1, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)34, slotUnion.slotItem.value);

        writer.mItemCount = 64;
        writer.AppendSlotItem(SIT_DELTA_UINT8, 321);
        slotUnion.slotValue = (*writer.mIndexArray)[1];
        ASSERT_EQ((uint64_t)0, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)321, slotUnion.slotItem.value);
    }

    {
        union SlotItemUnion
        {
            LongSlotItem slotItem;
            uint64_t slotValue;
        };
        SlotItemUnion slotUnion;

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        writer.AppendSlotItem(SIT_EQUAL, 34);
        slotUnion.slotValue = (*writer.mIndexArray)[0];
        ASSERT_EQ((uint64_t)1, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)34, slotUnion.slotItem.value);

        writer.mItemCount = 64;
        writer.AppendSlotItem(SIT_DELTA_UINT8, 321);
        slotUnion.slotValue = (*writer.mIndexArray)[1];
        ASSERT_EQ((uint64_t)0, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)321, slotUnion.slotItem.value);
    }
}

void EquivalentCompressWriterTest::TestFlushSlotBuffer()
{
    {
        uint32_t slotBufferItems[] = {1, 2, 3, 4, 5, 6, 7};
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.mCursorInBuffer = 7;
        memcpy(writer.mSlotBuffer, slotBufferItems, 7 * sizeof(uint32_t));

        writer.FlushSlotBuffer();
        ASSERT_EQ((uint32_t)0, writer.mCursorInBuffer);
        ASSERT_EQ((uint32_t)7, writer.mItemCount);
        ASSERT_EQ((size_t)8, writer.mDataArrayCursor);
    }
    {
        uint32_t slotBufferItems[] = {1, 2, 3, 4, 5, 6, 17};
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.mCursorInBuffer = 7;
        memcpy(writer.mSlotBuffer, slotBufferItems, 7 * sizeof(uint32_t));

        writer.FlushSlotBuffer();
        ASSERT_EQ((uint32_t)0, writer.mCursorInBuffer);
        ASSERT_EQ((uint32_t)7, writer.mItemCount);
        ASSERT_EQ((size_t)11, writer.mDataArrayCursor);
    }
    {
        uint32_t slotBufferItems[] = {1, 1, 1, 1, 1, 1, 1};
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.mCursorInBuffer = 7;
        memcpy(writer.mSlotBuffer, slotBufferItems, 7 * sizeof(uint32_t));

        writer.FlushSlotBuffer();
        ASSERT_EQ((uint32_t)0, writer.mCursorInBuffer);
        ASSERT_EQ((uint32_t)7, writer.mItemCount);
        ASSERT_EQ((size_t)0, writer.mDataArrayCursor);
    }
}

void EquivalentCompressWriterTest::TestCalculateDeltaBufferSize()
{
    {
        uint32_t buffer[] = {1, 1, 1, 1, 1, 1, 20};
        ASSERT_EQ((size_t)0, EquivalentCompressWriter<uint32_t>::CalculateDeltaBufferSize(buffer, 6));
        ASSERT_EQ((size_t)11, EquivalentCompressWriter<uint32_t>::CalculateDeltaBufferSize(buffer, 7));
    }

    {
        uint64_t buffer[] = {1, 1, 1, 1, 1, 1, 20};
        ASSERT_EQ((size_t)0, EquivalentCompressWriter<uint64_t>::CalculateDeltaBufferSize(buffer, 6));
        ASSERT_EQ((size_t)23, EquivalentCompressWriter<uint64_t>::CalculateDeltaBufferSize(buffer, 7));
    }

    {
        uint64_t buffer[] = {1, 1, 1, 1, 1, 1, 20};
        ASSERT_EQ((size_t)0, EquivalentCompressWriter<int64_t>::CalculateDeltaBufferSize(buffer, 6));
        ASSERT_EQ((size_t)23, EquivalentCompressWriter<int64_t>::CalculateDeltaBufferSize(buffer, 7));
    }
}

void EquivalentCompressWriterTest::TestCalculateCompressLength()
{
    {
        uint32_t value[150] = {0};
        value[0] = 66;   // one byte
        value[64] = 823; // two bytes

        size_t compSize = EquivalentCompressWriter<uint32_t>::CalculateCompressLength(value, 150, 64);
        ASSERT_EQ(8 + 3*sizeof(uint64_t) + 2*sizeof(uint32_t) + 64*sizeof(uint8_t) + 64*sizeof(uint16_t), compSize);
    }

    {
        uint64_t value[150] = {0};
        value[0] = 66;   // one byte
        value[64] = (uint64_t)1 << 63; // eight bytes

        size_t compSize = EquivalentCompressWriter<uint64_t>::CalculateCompressLength(value, 150, 64);
        ASSERT_EQ(8 + 3*sizeof(uint64_t) + 2*2*sizeof(uint64_t) + 64*sizeof(uint8_t) + 64*sizeof(uint64_t), compSize);
    }

    {
        int64_t value1[80] = {0};
        value1[0] = -17;
        value1[65] = 17;
        int64_t value2[40] = {0};
        value2[0] = (uint64_t)1 << 63;
        
        EquivalentCompressWriter<int64_t> writer;
        writer.Init(64);
        writer.CompressData(value1, 80);
        writer.CompressData(value2, 40);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<int64_t> reader(buffer);
        size_t calculatedCompressSize = EquivalentCompressWriter<int64_t>::CalculateCompressLength(reader, 64);
        ASSERT_EQ(compressSize, calculatedCompressSize);

        calculatedCompressSize = EquivalentCompressWriter<int64_t>::CalculateCompressLength(reader, 65);
        ASSERT_FALSE(compressSize == calculatedCompressSize);
        delete[] buffer;   
    }
}

void EquivalentCompressWriterTest::TestCompressData()
{
    {   // test uint32
        uint32_t value1[] = {17, 2, 3, 9, 17, 5};
        uint32_t value2[] = {23, 68, 281, 321, 3, 647, 
                             823, 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<uint32_t> reader(buffer);
        
        size_t i = 0;
        for (; i < 6; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }

        for (; i < 20; i++)
        {
            ASSERT_EQ(value2[i - 6], reader[i]);
        }
        
        delete[] buffer;
    }

    {   // test float, delta compress
        float value1[] = {17.01, 2.01, 3.03, 9.04, 17.05, 5.06};
        float value2[] = {23, 68, 281, 321, 3, 647.18, 
                             823, 31, 478, 83.9, 3, 3, 3, 3};

        EquivalentCompressWriter<float> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<float> reader(buffer);
        
        size_t i = 0;
        for (; i < 6; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }

        for (; i < 20; i++)
        {
            ASSERT_EQ(value2[i - 6], reader[i]);
        }
        
        delete[] buffer;
    }

    {   // test float, equal compress
        float value1[10] = {17.0112};
        EquivalentCompressWriter<float> writer;
        writer.Init(8);
        writer.CompressData(value1, 10);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<float> reader(buffer);
        for (size_t i = 0; i < 10; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }
        delete[] buffer;
    }

    {   // test double, equal compress
        double value1[10] = {171233.011282};
        EquivalentCompressWriter<double> writer;
        writer.Init(8);
        writer.CompressData(value1, 10);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<double> reader(buffer);
        for (size_t i = 0; i < 10; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }
        delete[] buffer;
    }

    {   // test uint64, delta compress
        uint64_t value1[] = {17, 2, 3, 9, 17, 5};
        uint64_t value2[] = {23, 68, 281, 321, 3, 647,
                             ((uint64_t)1 << 63), 31, 478,
                             83, 3, 3, 3, 3};
        
        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<uint64_t> reader(buffer);
        
        size_t i = 0;
        for (; i < 6; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }

        for (; i < 20; i++)
        {
            ASSERT_EQ(value2[i - 6], reader[i]);
        }
        
        delete[] buffer;
    }

    {   // test double, delta compress
        double value1[] = {17, 2, 3, 9, 17.12834, 5};
        double value2[] = {23, 68, 281, 321, 3, 647,
                             123412.1234213, 31, 478.123,
                             83, 3, 3, 3, 3};
        
        EquivalentCompressWriter<double> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<double> reader(buffer);
        
        size_t i = 0;
        for (; i < 6; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }

        for (; i < 20; i++)
        {
            ASSERT_EQ(value2[i - 6], reader[i]);
        }
        
        delete[] buffer;
    }

    {   // test int64, delta compress
        int64_t value1[] = {-17, 2, -3, 9, 17, 5};
        int64_t value2[] = {23, 68, -281, 321, 3, 647,
			    (int64_t)((uint64_t)1 << 63), 31, 478,
                             83, 3, 3, 3, 3};
        
        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<int64_t> reader(buffer);
        
        size_t i = 0;
        for (; i < 6; i++)
        {
            ASSERT_EQ(value1[i], reader[i]);
        }

        for (; i < 20; i++)
        {
            ASSERT_EQ(value2[i - 6], reader[i]);
        }
        
        delete[] buffer;
    }

    {   // test int64, delta compress, compress use reader
        int64_t value1[] = {-17, 2, -3, 9, 17, 5};
        int64_t value2[] = {23, 68, -281, 321, 3, 647,
			    (int64_t)((uint64_t)1 << 63), 31, 478,
                             83, 3, 3, 3, 3};
        
        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t *buffer = new uint8_t[compressSize];
        writer.DumpBuffer(buffer, compressSize);
        EquivalentCompressReader<int64_t> reader(buffer);
        
        writer.Init(10);
        writer.CompressData(reader);
        compressSize = writer.GetCompressLength();
        delete[] buffer;
        buffer = new uint8_t[compressSize];
        writer.DumpBuffer(buffer, compressSize);
        EquivalentCompressReader<int64_t> reader1(buffer);

        ASSERT_EQ((uint32_t)20, reader1.Size());
        size_t i = 0;
        for (; i < 6; i++)
        {
            ASSERT_EQ(value1[i], reader1[i]);
        }

        for (; i < 20; i++)
        {
            ASSERT_EQ(value2[i - 6], reader1[i]);
        }
        delete[] buffer;
    }
}

void EquivalentCompressWriterTest::TestSimpleBitCompress()
{
    const size_t valueCount = 65;
    uint32_t value1[valueCount] = {0};
    value1[0] = 2;
        
    ASSERT_EQ((size_t)44,  
              EquivalentCompressWriter<uint32_t>::CalculateCompressLength(
                      value1, valueCount, 64));

    EquivalentCompressWriter<uint32_t> writer;
    writer.Init(64);
    writer.CompressData(value1, valueCount);
    size_t compressSize = writer.GetCompressLength();
    uint8_t *buffer = new uint8_t[compressSize];
    ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));
    // 44 = 2 * 4 (header) + 2 * 8 (meta) + (4 + 64 * 2 / 8) (slot)
    ASSERT_EQ((size_t)44, compressSize);

    EquivalentCompressReader<uint32_t> reader(buffer);
    for (size_t i = 0; i < valueCount; ++i)
    {
        ASSERT_EQ(value1[i], reader[i]);
    }
    delete[] buffer;   
}

void EquivalentCompressWriterTest::TestBitCompress()
{
    TestTypedBitCompress<uint8_t, 2>();
    TestTypedBitCompress<uint8_t, 4>();
    TestTypedBitCompress<uint8_t, 16>();

    TestTypedBitCompress<int8_t, 2>();
    TestTypedBitCompress<int8_t, 4>();
    TestTypedBitCompress<int8_t, 16>();

    TestTypedBitCompress<uint16_t, 2>();
    TestTypedBitCompress<uint16_t, 4>();
    TestTypedBitCompress<uint16_t, 16>();

    TestTypedBitCompress<int16_t, 2>();
    TestTypedBitCompress<int16_t, 4>();
    TestTypedBitCompress<int16_t, 16>();

    TestTypedBitCompress<uint32_t, 2>();
    TestTypedBitCompress<uint32_t, 4>();
    TestTypedBitCompress<uint32_t, 16>();

    TestTypedBitCompress<int32_t, 2>();
    TestTypedBitCompress<int32_t, 4>();
    TestTypedBitCompress<int32_t, 16>();

    TestTypedBitCompress<uint64_t, 2>();
    TestTypedBitCompress<uint64_t, 4>();
    TestTypedBitCompress<uint64_t, 16>();

    TestTypedBitCompress<int64_t, 2>();
    TestTypedBitCompress<int64_t, 4>();
    TestTypedBitCompress<int64_t, 16>();

    TestTypedBitCompress<float, 2>();
    TestTypedBitCompress<float, 4>();
    TestTypedBitCompress<float, 16>();

    TestTypedBitCompress<double, 2>();
    TestTypedBitCompress<double, 4>();
    TestTypedBitCompress<double, 16>();
}

void EquivalentCompressWriterTest::CheckDumpedFile(const string& filePath)
{
    ASSERT_EQ((size_t)24, storage::FileSystemWrapper::GetFileLength(filePath));

    string fileContent;
    storage::FileSystemWrapper::AtomicLoad(filePath, fileContent);
    uint8_t * buffer = (uint8_t*)fileContent.data();

    ASSERT_EQ(*(uint32_t*)buffer, (uint32_t)1);
    ASSERT_EQ(*(uint32_t*)(buffer + 4), (uint32_t)3);
    ASSERT_EQ(*(uint64_t*)(buffer + 8), (uint64_t)1);
    ASSERT_EQ(*(uint64_t*)(buffer + 16), (uint64_t)1);
}

void EquivalentCompressWriterTest::PrepareMockWriterForDump(
        MockEquivalentCompressWriter& writer)
{
    typedef EquivalentCompressWriterBase::IndexArray IndexArray;
    typedef EquivalentCompressWriterBase::DataArray DataArray;
    
    writer.mItemCount = 1;
    writer.mSlotItemCount = 8;
    writer.mSlotItemPowerNum = 3;
    writer.mDataArrayCursor = 8;
    writer.mIndexArray = new IndexArray(2, 1, NULL);
    writer.mDataArray = new DataArray(2, 1, NULL);
    writer.mIndexArray->Set(0, 1);
    writer.mDataArray->SetTypedValue<uint64_t>(0, 1);
}

IE_NAMESPACE_END(common);

