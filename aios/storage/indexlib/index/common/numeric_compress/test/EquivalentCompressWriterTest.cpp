#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "unittest/unittest.h"

using ::testing::Return;

using namespace indexlib;
using namespace indexlib::file_system;
using namespace std;

namespace indexlib::index {

class EquivalentCompressWriterTest : public TESTBASE
{
public:
    class MockEquivalentCompressWriter : public EquivalentCompressWriterBase
    {
    public:
        MOCK_METHOD(void, InitSlotBuffer, (), (override));
        MOCK_METHOD(void, FreeSlotBuffer, (), (override));
        MOCK_METHOD(void, FlushSlotBuffer, (), (override));
    };

public:
    EquivalentCompressWriterTest() = default;
    ~EquivalentCompressWriterTest() = default;

private:
    void PrepareMockWriterForDump(MockEquivalentCompressWriter& mockWriter);
    void CheckDumpedFile(const std::string& filePath);

    template <typename T, int maxValueInTailSlot>
    void TestTypedBitCompress()
    {
        const size_t valueCount = 300;
        T value[valueCount] = {0};

        for (size_t i = 0; i < 64; i++) // first slot data, 1 bit slot
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

        value[192] = 30; // fourth 1 byte slot

        assert(maxValueInTailSlot <= 16);
        for (size_t i = 256; i < 300; i++) // fifth slot data
        {
            value[i] = (T)(i % maxValueInTailSlot);
        }

        auto [status, calculateLen] = EquivalentCompressWriter<T>::CalculateCompressLength(value, valueCount, 64);
        ASSERT_TRUE(status.IsOK());

        EquivalentCompressWriter<T> writer;
        writer.Init(64);
        writer.CompressData(value, valueCount);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));
        ASSERT_EQ(compressSize, calculateLen);

        EquivalentCompressReader<T> reader(buffer);
        for (size_t i = 0; i < valueCount; ++i) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value[i], curVal);
        }
        delete[] buffer;
    }
};

void EquivalentCompressWriterTest::CheckDumpedFile(const string& filePath)
{
    ASSERT_EQ((size_t)24, file_system::FslibWrapper::GetFileLength(filePath).GetOrThrow());

    string fileContent;
    file_system::FslibWrapper::AtomicLoadE(filePath, fileContent);
    uint8_t* buffer = (uint8_t*)fileContent.data();

    ASSERT_EQ(*(uint32_t*)buffer, (uint32_t)1);
    ASSERT_EQ(*(uint32_t*)(buffer + 4), (uint32_t)3);
    ASSERT_EQ(*(uint64_t*)(buffer + 8), (uint64_t)1);
    ASSERT_EQ(*(uint64_t*)(buffer + 16), (uint64_t)1);
}

void EquivalentCompressWriterTest::PrepareMockWriterForDump(MockEquivalentCompressWriter& writer)
{
    typedef EquivalentCompressWriterBase::IndexArray IndexArray;
    typedef EquivalentCompressWriterBase::DataArray DataArray;

    writer._itemCount = 1;
    writer._slotItemCount = 8;
    writer._slotItemPowerNum = 3;
    writer._dataArrayCursor = 8;
    writer._indexArray = new IndexArray(2, 1, NULL);
    writer._dataArray = new DataArray(2, 1, NULL);
    writer._indexArray->Set(0, 1);
    writer._dataArray->SetTypedValue<uint64_t>(0, 1);
}

TEST_F(EquivalentCompressWriterTest, TestReset)
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FreeSlotBuffer());

    writer.Reset();
    ASSERT_EQ((uint32_t)0, writer._slotItemPowerNum);
    ASSERT_EQ((uint32_t)0, writer._slotItemCount);
    ASSERT_EQ((uint32_t)0, writer._itemCount);
    ASSERT_EQ((uint32_t)0, writer._dataArrayCursor);
    ASSERT_TRUE(writer._indexArray == NULL);
    ASSERT_TRUE(writer._dataArray == NULL);
}

TEST_F(EquivalentCompressWriterTest, TestInit)
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FreeSlotBuffer()).Times(2);
    EXPECT_CALL(writer, InitSlotBuffer()).Times(2);
    EXPECT_CALL(writer, FlushSlotBuffer()).Times(0);

    writer.Init(7);
    ASSERT_EQ((uint32_t)6, writer._slotItemPowerNum);
    ASSERT_EQ((uint32_t)64, writer._slotItemCount);
    ASSERT_EQ((uint32_t)0, writer._itemCount);
    ASSERT_EQ((uint32_t)0, writer._dataArrayCursor);
    ASSERT_TRUE(writer._indexArray != NULL);
    ASSERT_TRUE(writer._dataArray != NULL);

    writer._itemCount = 8;
    writer._dataArrayCursor = 18;

    writer.Init(255);
    ASSERT_EQ((uint32_t)8, writer._slotItemPowerNum);
    ASSERT_EQ((uint32_t)256, writer._slotItemCount);
    ASSERT_EQ((uint32_t)0, writer._itemCount);
    ASSERT_EQ((uint32_t)0, writer._dataArrayCursor);
    ASSERT_TRUE(writer._indexArray != NULL);
    ASSERT_TRUE(writer._dataArray != NULL);
}

TEST_F(EquivalentCompressWriterTest, TestGetCompressLength)
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer()).Times(2);

    {
        writer._itemCount = 0;
        ASSERT_EQ((size_t)8, writer.GetCompressLength());
    }

    {
        writer._itemCount = 127;
        writer._slotItemCount = 32;
        writer._dataArrayCursor = 10;
        // 8 + 8 * 4 + 10
        ASSERT_EQ((size_t)50, writer.GetCompressLength());
    }
}

TEST_F(EquivalentCompressWriterTest, TestGetUpperPowerNumber)
{
    MockEquivalentCompressWriter writer;
    ASSERT_EQ((uint32_t)6, writer.GetUpperPowerNumber(3));
    ASSERT_EQ((uint32_t)6, writer.GetUpperPowerNumber(8));
    ASSERT_EQ((uint32_t)7, writer.GetUpperPowerNumber(127));
    ASSERT_EQ((uint32_t)10, writer.GetUpperPowerNumber(1024));
    ASSERT_EQ((uint32_t)11, writer.GetUpperPowerNumber(1025));
}

TEST_F(EquivalentCompressWriterTest, TestDumpBuffer)
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer()).WillRepeatedly(Return());
    PrepareMockWriterForDump(writer);

    uint8_t buffer[30];
    char* base = (char*)buffer;
    ASSERT_EQ(size_t(24), writer.DumpBuffer(buffer, sizeof(buffer)));

    ASSERT_EQ(*(uint32_t*)base, (uint32_t)1);
    ASSERT_EQ(*(uint32_t*)(base + 4), (uint32_t)3);
    ASSERT_EQ(*(uint64_t*)(base + 8), (uint64_t)1);
    ASSERT_EQ(*(uint64_t*)(base + 16), (uint64_t)1);
}

TEST_F(EquivalentCompressWriterTest, TestDumpFile)
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer()).WillRepeatedly(Return());
    PrepareMockWriterForDump(writer);

    string filePath = GET_TEMP_DATA_PATH() + "equivalent_compress_writer_test_dump_file";
    BufferedFileWriterPtr fileWriter(new BufferedFileWriter);
    ASSERT_EQ(FSEC_OK, fileWriter->Open(filePath, filePath));
    auto st = writer.Dump(fileWriter);
    ASSERT_TRUE(st.IsOK());
    fileWriter->Close().GetOrThrow();

    CheckDumpedFile(filePath);
}

TEST_F(EquivalentCompressWriterTest, TestDump)
{
    MockEquivalentCompressWriter writer;
    EXPECT_CALL(writer, FlushSlotBuffer()).WillRepeatedly(Return());
    PrepareMockWriterForDump(writer);

    file_system::FileWriterPtr fileWriter = Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH())
                                                ->CreateFileWriter("equivalent_compress_writer_test_dump_file");

    string filePath = fileWriter->GetPhysicalPath();
    auto st = writer.Dump(fileWriter);
    ASSERT_TRUE(st.IsOK());
    fileWriter->Close().GetOrThrow();
    CheckDumpedFile(filePath);
}

TEST_F(EquivalentCompressWriterTest, TestGetMaxDeltaInBuffer)
{
    uint32_t minValue = 0;
    uint32_t maxDelta = 0;
    {
        uint32_t value[] = {17, 2, 3, 9, 17};
        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(value, 1, minValue);
        ASSERT_EQ((uint32_t)0, maxDelta);
        ASSERT_EQ((uint32_t)17, minValue);

        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(value, 4, minValue);
        ASSERT_EQ((uint32_t)15, maxDelta);
        ASSERT_EQ((uint32_t)2, minValue);

        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(value, 5, minValue);
        ASSERT_EQ((uint32_t)15, maxDelta);
        ASSERT_EQ((uint32_t)2, minValue);
    }

    {
        uint32_t value[] = {1, 1, 1, 1, 1};
        maxDelta = EquivalentCompressWriter<uint32_t>::GetMaxDeltaInBuffer(value, 5, minValue);
        ASSERT_EQ((uint32_t)0, maxDelta);
        ASSERT_EQ((uint32_t)1, minValue);
    }
}

TEST_F(EquivalentCompressWriterTest, TestConvertToDeltaBuffer)
{
    uint32_t value[] = {17, 2, 3, 9, 17};
    uint32_t deltaValue[] = {15, 0, 1, 7, 15};

    EquivalentCompressWriter<uint32_t>::ConvertToDeltaBuffer(value, 5, 2);

    for (size_t i = 0; i < 5; ++i) {
        ASSERT_EQ(deltaValue[i], value[i]);
    }
}

TEST_F(EquivalentCompressWriterTest, TestGetMinDeltaBufferSize)
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

TEST_F(EquivalentCompressWriterTest, TestFlushDeltaBuffer)
{
    {
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);

        uint32_t deltaBuffer[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint32_t expectDelta[] = {6, 18, 31, 124, 0, 31, 7, 10};
        uint32_t minValue = 2;

        size_t size = writer.FlushDeltaBuffer(deltaBuffer, 8, minValue, SIT_DELTA_UINT8);

        uint32_t value;
        writer._dataArray->GetTypedValue<uint32_t>(0, value);
        ASSERT_EQ(minValue, value);

        uint8_t deltaValue;
        for (uint32_t i = 0; i < 8; ++i) {
            writer._dataArray->GetTypedValue<uint8_t>(4 + i, deltaValue);
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
        writer._dataArray->GetTypedValue<uint64_t>(0, value);
        ASSERT_EQ(minValue, value);
        writer._dataArray->GetTypedValue<uint64_t>(8, value);
        ASSERT_EQ(deltaPowerNum, value);

        uint16_t deltaValue;
        for (uint32_t i = 0; i < 8; ++i) {
            writer._dataArray->GetTypedValue<uint16_t>(16 + sizeof(uint16_t) * i, deltaValue);
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
        writer._dataArray->GetTypedValue<uint64_t>(0, value);
        ASSERT_EQ(minValue, value);
        writer._dataArray->GetTypedValue<uint64_t>(8, value);
        ASSERT_EQ(deltaPowerNum, value);

        uint16_t deltaValue;
        for (uint32_t i = 0; i < 8; ++i) {
            writer._dataArray->GetTypedValue<uint16_t>(16 + sizeof(uint16_t) * i, deltaValue);
            ASSERT_EQ(expectDelta[i], (uint64_t)deltaValue);
        }
        ASSERT_EQ((size_t)32, size);
    }
}

TEST_F(EquivalentCompressWriterTest, TestNeedStoreDelta)
{
    ASSERT_TRUE(EquivalentCompressWriter<uint32_t>::NeedStoreDelta(1, 3));
    ASSERT_FALSE(EquivalentCompressWriter<uint32_t>::NeedStoreDelta(0, 3));

    ASSERT_TRUE(EquivalentCompressWriter<uint64_t>::NeedStoreDelta(1, 3));
    ASSERT_TRUE(EquivalentCompressWriter<uint64_t>::NeedStoreDelta(0, std::numeric_limits<int64_t>::max() + 1ul));
    ASSERT_FALSE(EquivalentCompressWriter<int64_t>::NeedStoreDelta(0, std::numeric_limits<int64_t>::max()));
    ASSERT_FALSE(EquivalentCompressWriter<uint64_t>::NeedStoreDelta(0, std::numeric_limits<int64_t>::max()));
}

TEST_F(EquivalentCompressWriterTest, TestAppendSlotItem)
{
    {
        union SlotItemUnion {
            SlotItem slotItem;
            uint64_t slotValue;
        };
        SlotItemUnion slotUnion;

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.AppendSlotItem(SIT_EQUAL, 34);
        slotUnion.slotValue = (*writer._indexArray)[0];

        ASSERT_EQ(SIT_EQUAL, slotUnion.slotItem.slotType);
        ASSERT_EQ((uint64_t)34, slotUnion.slotItem.value);

        writer._itemCount = 64;
        writer.AppendSlotItem(SIT_DELTA_UINT8, 321);
        slotUnion.slotValue = (*writer._indexArray)[1];

        ASSERT_EQ(SIT_DELTA_UINT8, slotUnion.slotItem.slotType);
        ASSERT_EQ((uint64_t)321, slotUnion.slotItem.value);
    }

    {
        union SlotItemUnion {
            LongSlotItem slotItem;
            uint64_t slotValue;
        };
        SlotItemUnion slotUnion;

        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);
        writer.AppendSlotItem(SIT_EQUAL, 34);
        slotUnion.slotValue = (*writer._indexArray)[0];
        ASSERT_EQ((uint64_t)1, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)34, slotUnion.slotItem.value);

        writer._itemCount = 64;
        writer.AppendSlotItem(SIT_DELTA_UINT8, 321);
        slotUnion.slotValue = (*writer._indexArray)[1];
        ASSERT_EQ((uint64_t)0, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)321, slotUnion.slotItem.value);
    }

    {
        union SlotItemUnion {
            LongSlotItem slotItem;
            uint64_t slotValue;
        };
        SlotItemUnion slotUnion;

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        writer.AppendSlotItem(SIT_EQUAL, 34);
        slotUnion.slotValue = (*writer._indexArray)[0];
        ASSERT_EQ((uint64_t)1, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)34, slotUnion.slotItem.value);

        writer._itemCount = 64;
        writer.AppendSlotItem(SIT_DELTA_UINT8, 321);
        slotUnion.slotValue = (*writer._indexArray)[1];
        ASSERT_EQ((uint64_t)0, slotUnion.slotItem.isValue);
        ASSERT_EQ((uint64_t)321, slotUnion.slotItem.value);
    }
}

TEST_F(EquivalentCompressWriterTest, TestFlushSlotBuffer)
{
    {
        uint32_t slotBufferItems[] = {1, 2, 3, 4, 5, 6, 7};
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer._cursorInBuffer = 7;
        memcpy(writer._slotBuffer, slotBufferItems, 7 * sizeof(uint32_t));

        writer.FlushSlotBuffer();
        ASSERT_EQ((uint32_t)0, writer._cursorInBuffer);
        ASSERT_EQ((uint32_t)7, writer._itemCount);
        ASSERT_EQ((size_t)8, writer._dataArrayCursor);
    }
    {
        uint32_t slotBufferItems[] = {1, 2, 3, 4, 5, 6, 17};
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer._cursorInBuffer = 7;
        memcpy(writer._slotBuffer, slotBufferItems, 7 * sizeof(uint32_t));

        writer.FlushSlotBuffer();
        ASSERT_EQ((uint32_t)0, writer._cursorInBuffer);
        ASSERT_EQ((uint32_t)7, writer._itemCount);
        ASSERT_EQ((size_t)11, writer._dataArrayCursor);
    }
    {
        uint32_t slotBufferItems[] = {1, 1, 1, 1, 1, 1, 1};
        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer._cursorInBuffer = 7;
        memcpy(writer._slotBuffer, slotBufferItems, 7 * sizeof(uint32_t));

        writer.FlushSlotBuffer();
        ASSERT_EQ((uint32_t)0, writer._cursorInBuffer);
        ASSERT_EQ((uint32_t)7, writer._itemCount);
        ASSERT_EQ((size_t)0, writer._dataArrayCursor);
    }
}

TEST_F(EquivalentCompressWriterTest, TestCalculateDeltaBufferSize)
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

TEST_F(EquivalentCompressWriterTest, TestCalculateCompressLength)
{
    {
        uint32_t value[150] = {0};
        value[0] = 66;   // one byte
        value[64] = 823; // two bytes

        auto [status, compSize] = EquivalentCompressWriter<uint32_t>::CalculateCompressLength(value, 150, 64);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(8 + 3 * sizeof(uint64_t) + 2 * sizeof(uint32_t) + 64 * sizeof(uint8_t) + 64 * sizeof(uint16_t),
                  compSize);
    }

    {
        uint64_t value[150] = {0};
        value[0] = 66;                 // one byte
        value[64] = (uint64_t)1 << 63; // eight bytes

        auto [status, compSize] = EquivalentCompressWriter<uint64_t>::CalculateCompressLength(value, 150, 64);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(8 + 3 * sizeof(uint64_t) + 2 * 2 * sizeof(uint64_t) + 64 * sizeof(uint8_t) + 64 * sizeof(uint64_t),
                  compSize);
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
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<int64_t> reader(buffer);
        auto [status, calculatedCompressSize] = EquivalentCompressWriter<int64_t>::CalculateCompressLength(reader, 64);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(compressSize, calculatedCompressSize);

        std::tie(status, calculatedCompressSize) =
            EquivalentCompressWriter<int64_t>::CalculateCompressLength(reader, 65);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(compressSize == calculatedCompressSize);
        delete[] buffer;
    }
}

TEST_F(EquivalentCompressWriterTest, TestCompressData)
{
    { // test uint32
        uint32_t value1[] = {17, 2, 3, 9, 17, 5};
        uint32_t value2[] = {23, 68, 281, 321, 3, 647, 823, 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<uint32_t> reader(buffer);

        size_t i = 0;
        for (; i < 6; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }

        for (; i < 20; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value2[i - 6], curVal);
        }

        delete[] buffer;
    }

    { // test float, delta compress
        float value1[] = {17.01, 2.01, 3.03, 9.04, 17.05, 5.06};
        float value2[] = {23, 68, 281, 321, 3, 647.18, 823, 31, 478, 83.9, 3, 3, 3, 3};

        EquivalentCompressWriter<float> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<float> reader(buffer);

        size_t i = 0;
        for (; i < 6; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }

        for (; i < 20; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value2[i - 6], curVal);
        }

        delete[] buffer;
    }

    { // test float, equal compress
        float value1[10] = {17.0112};
        EquivalentCompressWriter<float> writer;
        writer.Init(8);
        writer.CompressData(value1, 10);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<float> reader(buffer);
        for (size_t i = 0; i < 10; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }
        delete[] buffer;
    }

    { // test double, equal compress
        double value1[10] = {171233.011282};
        EquivalentCompressWriter<double> writer;
        writer.Init(8);
        writer.CompressData(value1, 10);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<double> reader(buffer);
        for (size_t i = 0; i < 10; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }
        delete[] buffer;
    }

    { // test uint64, delta compress
        uint64_t value1[] = {17, 2, 3, 9, 17, 5};
        uint64_t value2[] = {23, 68, 281, 321, 3, 647, ((uint64_t)1 << 63), 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<uint64_t> reader(buffer);

        size_t i = 0;
        for (; i < 6; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }

        for (; i < 20; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value2[i - 6], curVal);
        }

        delete[] buffer;
    }

    { // test double, delta compress
        double value1[] = {17, 2, 3, 9, 17.12834, 5};
        double value2[] = {23, 68, 281, 321, 3, 647, 123412.1234213, 31, 478.123, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<double> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<double> reader(buffer);

        size_t i = 0;
        for (; i < 6; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }

        for (; i < 20; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value2[i - 6], curVal);
        }

        delete[] buffer;
    }

    { // test int64, delta compress
        int64_t value1[] = {-17, 2, -3, 9, 17, 5};
        int64_t value2[] = {23, 68, -281, 321, 3, 647, (int64_t)((uint64_t)1 << 63), 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));

        EquivalentCompressReader<int64_t> reader(buffer);

        size_t i = 0;
        for (; i < 6; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }

        for (; i < 20; i++) {
            auto [status, curVal] = reader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value2[i - 6], curVal);
        }

        delete[] buffer;
    }

    { // test int64, delta compress, compress use reader
        int64_t value1[] = {-17, 2, -3, 9, 17, 5};
        int64_t value2[] = {23, 68, -281, 321, 3, 647, (int64_t)((uint64_t)1 << 63), 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        writer.CompressData(value1, 6);
        writer.CompressData(value2, 14);
        size_t compressSize = writer.GetCompressLength();
        uint8_t* buffer = new uint8_t[compressSize];
        writer.DumpBuffer(buffer, compressSize);
        EquivalentCompressReader<int64_t> reader(buffer);

        writer.Init(10);
        ASSERT_TRUE(writer.CompressData(reader).IsOK());
        compressSize = writer.GetCompressLength();
        delete[] buffer;
        buffer = new uint8_t[compressSize];
        writer.DumpBuffer(buffer, compressSize);
        EquivalentCompressReader<int64_t> reader1(buffer);

        ASSERT_EQ((uint32_t)20, reader1.Size());
        size_t i = 0;
        for (; i < 6; i++) {
            auto [status, curVal] = reader1[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value1[i], curVal);
        }

        for (; i < 20; i++) {
            auto [status, curVal] = reader1[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(value2[i - 6], curVal);
        }
        delete[] buffer;
    }
}

TEST_F(EquivalentCompressWriterTest, TestSimpleBitCompress)
{
    const size_t valueCount = 65;
    uint32_t value1[valueCount] = {0};
    value1[0] = 2;

    auto [status, length] = EquivalentCompressWriter<uint32_t>::CalculateCompressLength(value1, valueCount, 64);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ((size_t)44, length);

    EquivalentCompressWriter<uint32_t> writer;
    writer.Init(64);
    writer.CompressData(value1, valueCount);
    size_t compressSize = writer.GetCompressLength();
    uint8_t* buffer = new uint8_t[compressSize];
    ASSERT_EQ(compressSize, writer.DumpBuffer(buffer, compressSize));
    // 44 = 2 * 4 (header) + 2 * 8 (meta) + (4 + 64 * 2 / 8) (slot)
    ASSERT_EQ((size_t)44, compressSize);

    EquivalentCompressReader<uint32_t> reader(buffer);
    for (size_t i = 0; i < valueCount; ++i) {
        auto [status, curVal] = reader[i];
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(value1[i], curVal);
    }
    delete[] buffer;
}

TEST_F(EquivalentCompressWriterTest, TestBitCompress)
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
} // namespace indexlib::index
