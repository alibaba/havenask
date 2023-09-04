#include "indexlib/index/common/data_structure/ExpandableValueAccessor.h"

#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Types.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class ExpandableValueAccessorTest : public TESTBASE
{
    class MockFileWriter : public indexlib::file_system::FileWriter
    {
    public:
        MockFileWriter() noexcept = default;
        ~MockFileWriter() noexcept = default;

    public:
        indexlib::file_system::FSResult<size_t> Write(const void* buffer, size_t length) noexcept override
        {
            _data.append(string((const char*)buffer, length));
            return indexlib::file_system::FSResult(indexlib::file_system::FSEC_OK, length);
        }
        void* GetBaseAddress() noexcept override { return _data.data(); }
        size_t GetLength() const noexcept override { return _data.size(); }

    private:
        const std::string& GetLogicalPath() const noexcept override
        {
            static string ret;
            return ret;
        }
        const std::string& GetPhysicalPath() const noexcept override
        {
            static string ret;
            return ret;
        }
        indexlib::file_system::FSResult<void> Open(const std::string& logicalPath,
                                                   const std::string& physicalPath) noexcept override
        {
            return indexlib::file_system::FSResult<void>(indexlib::file_system::FSEC_OK);
        }
        indexlib::file_system::FSResult<void> Close() noexcept override
        {
            return indexlib::file_system::FSResult<void>(indexlib::file_system::FSEC_OK);
        }
        const indexlib::file_system::WriterOption& GetWriterOption() const noexcept override
        {
            static indexlib::file_system::WriterOption option;
            return option;
        }
        indexlib::file_system::FSResult<void> ReserveFile(size_t reserveSize) noexcept override
        {
            return indexlib::file_system::FSResult<void>(indexlib::file_system::FSEC_OK);
        }
        indexlib::file_system::FSResult<void> Truncate(size_t truncateSize) noexcept override
        {
            return indexlib::file_system::FSResult<void>(indexlib::file_system::FSEC_OK);
        }

    private:
        std::string _data;
    };

public:
    void setUp() override
    {
        _pool = std::make_shared<autil::mem_pool::Pool>();
        assert(!!_pool);
    }
    void tearDown() override {}

public:
    template <typename T>
    void DoTestInitSingleSlice()
    {
        ExpandableValueAccessor<T> valueAccessor(_pool.get(), true);
        ASSERT_TRUE(valueAccessor.Init(/*sliceLen*/ 1000, /*sliceNum*/ 1).IsOK());
        ASSERT_EQ(1000, valueAccessor.GetReserveBytes());
        ASSERT_EQ(0, valueAccessor.GetUsedBytes());
        ASSERT_EQ(1, valueAccessor._sliceNum);
        ASSERT_EQ(1000, valueAccessor._sliceLen);
        ASSERT_EQ(0, valueAccessor._baseOffset);
        ASSERT_EQ(0, valueAccessor._curAccessorIdx);
        ASSERT_EQ(true, valueAccessor._enableRewrite);
        ASSERT_NE(nullptr, valueAccessor._sliceAllocator);
        ASSERT_EQ(1000, valueAccessor._sliceAllocator->GetSliceLen());
        ASSERT_EQ(1, valueAccessor._sliceAllocator->GetSliceNum());
        ASSERT_EQ(1, valueAccessor._valueAccessors.size());
    }

    template <typename T>
    void DoTestInitMultiSlices()
    {
        ExpandableValueAccessor<T> valueAccessor(_pool.get(), true);
        ASSERT_TRUE(valueAccessor.Init(/*sliceLen*/ 1024, /*sliceNum*/ 2).IsOK());
        ASSERT_EQ(1024 * 2, valueAccessor.GetReserveBytes());
        ASSERT_EQ(0, valueAccessor.GetUsedBytes());
        ASSERT_EQ(2, valueAccessor._sliceNum);
        ASSERT_EQ(1024, valueAccessor._sliceLen);
        ASSERT_EQ(0, valueAccessor._baseOffset);
        ASSERT_EQ(0, valueAccessor._curAccessorIdx);
        ASSERT_EQ(true, valueAccessor._enableRewrite);
        ASSERT_NE(nullptr, valueAccessor._sliceAllocator);
        ASSERT_EQ(1024, valueAccessor._sliceAllocator->GetSliceLen());
        ASSERT_EQ(1, valueAccessor._sliceAllocator->GetSliceNum());
        ASSERT_EQ(2, valueAccessor._valueAccessors.size());
    }

    template <typename T, int shiftBit>
    void DoTestReserveBytesOverflow()
    {
        ExpandableValueAccessor<T> valueAccessor(_pool.get(), true);
        T sliceLen = static_cast<T>(1) << shiftBit;
        size_t sliceNum = 2;
        ASSERT_FALSE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    }

    void PerfGetValue()
    {
        ExpandableValueAccessor<short_offset_t> expandableValueWriter(_pool.get(), true);
        size_t sliceLen = 64ul * 1024ul * 1024ul;
        ASSERT_TRUE(expandableValueWriter.Init(sliceLen, 1).IsOK());

        indexlib::util::ValueWriter<short_offset_t> valueAccessor(true);
        char* slice = reinterpret_cast<char*>(_pool->allocate(sliceLen));
        ASSERT_NE(nullptr, slice);
        valueAccessor.Init(slice, sliceLen);

        for (size_t i = 0; i < sliceLen / 64; ++i) {
            std::string value(63, '\1');
            valueAccessor.Append(autil::StringView(value.data(), value.size() + 1));
            short_offset_t _;
            ASSERT_TRUE(expandableValueWriter.Append(autil::StringView(value.data(), value.size() + 1), _).IsOK());
        }

        size_t sum = 0;
        size_t startTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < 1024 * 1024; ++i) {
            sum += (size_t)valueAccessor.GetValue(i * 64);
        }
        size_t endTime = autil::TimeUtility::currentTime();
        std::cout << "ValueWriter time cost  " << endTime - startTime << std::endl;

        startTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < 1024 * 1024; ++i) {
            sum += (size_t)expandableValueWriter.GetValue(i * 64);
        }
        endTime = autil::TimeUtility::currentTime();
        std::cout << "ExpandableValueAccessor time cost  " << endTime - startTime << std::endl;
    }

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
};

TEST_F(ExpandableValueAccessorTest, TestInitSingleSlice)
{
    DoTestInitSingleSlice<short_offset_t>();
    DoTestInitSingleSlice<offset_t>();
}

TEST_F(ExpandableValueAccessorTest, TestInitMultiSlices)
{
    DoTestInitMultiSlices<short_offset_t>();
    DoTestInitMultiSlices<offset_t>();
}

TEST_F(ExpandableValueAccessorTest, TestReserveBytesOverflow)
{
    DoTestReserveBytesOverflow<short_offset_t, 31>();
    DoTestReserveBytesOverflow<offset_t, 63>();
}

TEST_F(ExpandableValueAccessorTest, TestSliceLenNotPowerOfTwo)
{
    {
        ExpandableValueAccessor<short_offset_t> valueAccessor(_pool.get(), true);
        short_offset_t sliceLen = 0;
        size_t sliceNum = 1;
        ASSERT_FALSE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    }
    {
        ExpandableValueAccessor<short_offset_t> valueAccessor(_pool.get(), true);
        short_offset_t sliceLen = 1023;
        size_t sliceNum = 2;
        ASSERT_TRUE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    }
}

TEST_F(ExpandableValueAccessorTest, TestValueLenOutOfRange)
{
    ExpandableValueAccessor<short_offset_t> valueAccessor(_pool.get(), true);
    short_offset_t sliceLen = 1024;
    size_t sliceNum = 2;
    ASSERT_TRUE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    ASSERT_EQ(1024 * 2, valueAccessor.GetReserveBytes());

    size_t size = 1025;
    string value(size - 1, '3');
    short_offset_t _;
    ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), _).IsOutOfRange());
}

TEST_F(ExpandableValueAccessorTest, TestOutOfMemory)
{
    ExpandableValueAccessor<short_offset_t> valueAccessor(_pool.get(), true);
    short_offset_t sliceLen = 1024;
    size_t sliceNum = 2;
    ASSERT_TRUE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    ASSERT_EQ(1024 * 2, valueAccessor.GetReserveBytes());

    // append
    {
        size_t size = 1024;
        string value(size - 1, '1');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(0, offset);
        ASSERT_EQ(1024, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(0);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // append
    {
        size_t size = 1024;
        string value(size - 1, '2');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(1024, offset);
        ASSERT_EQ(2048, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(1024);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }

    size_t size = 1;
    string value;
    short_offset_t _;
    ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), _).IsNoMem());
}

TEST_F(ExpandableValueAccessorTest, TestSingleSlice)
{
    ExpandableValueAccessor<short_offset_t> valueAccessor(_pool.get(), true);
    short_offset_t sliceLen = 1000;
    size_t sliceNum = 1;
    ASSERT_TRUE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    ASSERT_EQ(1000, valueAccessor.GetReserveBytes());
    ASSERT_EQ(1000, valueAccessor.GetExpandMemoryInBytes());
    // append
    {
        size_t size = 10;
        string value(size - 1, '1');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(0, offset);
        ASSERT_EQ(10, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(0);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // append
    {
        size_t size = 20;
        string value(size - 1, '2');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(10, offset);
        ASSERT_EQ(30, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(10);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // rewrite
    {
        size_t size = 10;
        string value(size - 1, '3');
        ASSERT_TRUE(valueAccessor.Rewrite(autil::StringView(value.data(), size), 0).IsOK());
        ASSERT_EQ(30, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(0);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // dump
    auto fileWriter = std::make_shared<MockFileWriter>();
    auto result = valueAccessor.Dump(fileWriter);
    ASSERT_TRUE(result.OK());
    ASSERT_EQ(30, result.Value());
    ASSERT_EQ(30, fileWriter->GetLength());
    auto baseAddr = reinterpret_cast<const char*>(fileWriter->GetBaseAddress());
    ASSERT_NE(nullptr, baseAddr);
    for (int32_t idx = 0; idx < 30; ++idx) {
        if (idx < 9) {
            ASSERT_EQ('3', baseAddr[idx]);
        } else if (idx == 9) {
            ASSERT_EQ('\0', baseAddr[idx]);
        } else if (idx < 29) {
            ASSERT_EQ('2', baseAddr[idx]);
        } else {
            ASSERT_EQ('\0', baseAddr[idx]);
        }
    }

    size_t size = 971;
    string value(size - 1, '4');
    short_offset_t _;
    ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), _).IsNoMem());
}

TEST_F(ExpandableValueAccessorTest, TestMultiSlices)
{
    ExpandableValueAccessor<short_offset_t> valueAccessor(_pool.get(), true);
    short_offset_t sliceLen = 1024;
    size_t sliceNum = 2;
    ASSERT_TRUE(valueAccessor.Init(sliceLen, sliceNum).IsOK());
    ASSERT_EQ(1024 * 2, valueAccessor.GetReserveBytes());
    ASSERT_EQ(1024, valueAccessor.GetExpandMemoryInBytes());
    // append
    {
        size_t size = 10;
        string value(size - 1, '1');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(0, offset);
        ASSERT_EQ(10, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(0);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // append
    {
        size_t size = 20;
        string value(size - 1, '2');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(10, offset);
        ASSERT_EQ(30, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(10);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // trigger expand
    {
        size_t size = 1000;
        string value(size - 1, '3');
        short_offset_t offset = 0;
        ASSERT_TRUE(valueAccessor.Append(autil::StringView(value.data(), size), offset).IsOK());
        ASSERT_EQ(1024, offset);
        ASSERT_EQ(1024 + 1000, valueAccessor.GetUsedBytes());
        ASSERT_EQ(1024 + 1024, valueAccessor.GetExpandMemoryInBytes());
        const char* valueGet = valueAccessor.GetValue(1024);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // rewrite
    {
        size_t size = 10;
        string value(size - 1, '4');
        ASSERT_TRUE(valueAccessor.Rewrite(autil::StringView(value.data(), size), 0).IsOK());
        ASSERT_EQ(1024 + 1000, valueAccessor.GetUsedBytes());
        const char* valueGet = valueAccessor.GetValue(0);
        ASSERT_NE(nullptr, valueGet);
        ASSERT_EQ(value, string(valueGet));
    }
    // dump
    auto fileWriter = std::make_shared<MockFileWriter>();
    auto result = valueAccessor.Dump(fileWriter);
    ASSERT_TRUE(result.OK());
    ASSERT_EQ(2024, result.Value());
    ASSERT_EQ(2024, fileWriter->GetLength());
    auto baseAddr = reinterpret_cast<const char*>(fileWriter->GetBaseAddress());
    ASSERT_NE(nullptr, baseAddr);
    for (int32_t idx = 0; idx < 2024; ++idx) {
        if (idx < 9) {
            ASSERT_EQ('4', baseAddr[idx]);
        } else if (idx == 9) {
            ASSERT_EQ('\0', baseAddr[idx]);
        } else if (idx < 29) {
            ASSERT_EQ('2', baseAddr[idx]);
        } else if (idx == 29) {
            ASSERT_EQ('\0', baseAddr[idx]);
        } else if (idx < 1024) {
            ASSERT_EQ('\0', baseAddr[idx]);
        } else if (idx < 2023) {
            ASSERT_EQ('3', baseAddr[idx]);
        } else {
            ASSERT_EQ('\0', baseAddr[idx]);
        }
    }
}

} // namespace indexlibv2::index
