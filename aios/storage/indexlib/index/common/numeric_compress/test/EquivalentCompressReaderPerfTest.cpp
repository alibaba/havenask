#include <random>

#include "autil/TimeUtility.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "unittest/unittest.h"

using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace autil;
using namespace std;

namespace indexlib::index {

class EquivalentCompressReaderPerfTest : public TESTBASE
{
public:
    EquivalentCompressReaderPerfTest() = default;
    ~EquivalentCompressReaderPerfTest() = default;

    void setUp() override;
    void tearDown() override {};

private:
    template <typename T>
    void MakeData(uint32_t count, std::vector<T>& valueArray, uint32_t ratio)
    {
        for (uint32_t i = 0; i < count; ++i) {
            T value = (T)rand();
            if ((uint32_t)(value % 100) < ratio) {
                valueArray.push_back((T)100);
            } else {
                valueArray.push_back(value);
            }
        }
    }

    template <typename T>
    void InnerTestInplaceUpdatePerf(T value, uint32_t itemCount, uint32_t slotItemCount, uint32_t updateTimes,
                                    uint32_t updateStep)
    {
        std::vector<T> valueArray;
        valueArray.resize(itemCount);
        for (size_t i = 0; i < itemCount; i++) {
            valueArray[i] = (i % 2 == 0) ? 0 : value;
        }

        EquivalentCompressWriter<T> writer;
        writer.Init(slotItemCount);

        auto [status, compressLength] = writer.CalculateCompressLength(valueArray, slotItemCount);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(valueArray);
        writer.DumpBuffer(buffer, compressLength);

        EquivalentCompressReader<T> compArray(buffer);
        size_t pos = 0;
        int64_t beginTime = autil::TimeUtility::currentTime();
        std::cout << "***** begin inplace update !******" << std::endl;

        // compress update
        for (size_t i = 0; i < updateTimes; i++) {
            ASSERT_TRUE(compArray.Update(pos, 0));
            pos = (pos + updateStep) % itemCount;
        }
        int64_t interval = autil::TimeUtility::currentTime() - beginTime;

        double updateQps = updateTimes / ((double)interval / 1000 / 1000);
        std::cout << "***** time interval : " << interval / 1000 << "ms, QPS:" << (int64_t)updateQps << std::endl;

        beginTime = autil::TimeUtility::currentTime();
        std::cout << "***** begin array update !******" << std::endl;

        // array update
        for (size_t i = 0; i < updateTimes; i++) {
            valueArray[pos] = 0;
            pos = (pos + updateStep) % itemCount;
        }
        interval = autil::TimeUtility::currentTime() - beginTime;
        updateQps = updateTimes / ((double)interval / 1000 / 1000);
        std::cout << "***** time interval : " << interval / 1000 << "ms, QPS:" << (int64_t)updateQps << std::endl;

        std::cout << "############################" << std::endl;
        delete[] buffer;
    }

    template <typename T>
    void InnerTestExpandUpdatePerf(uint32_t itemCount, uint32_t slotItemCount, uint32_t updateStep)
    {
        assert(sizeof(T) == sizeof(uint32_t) || sizeof(T) == sizeof(uint64_t));

        T maxValue = std::numeric_limits<uint32_t>::max();
        size_t updateTimes = 65536; // ensure not expand reach to max slot type
        if (sizeof(T) == sizeof(uint64_t)) {
            maxValue = (T)std::numeric_limits<uint64_t>::max();
        }

        std::vector<T> valueArray;
        valueArray.resize(itemCount);
        for (size_t i = 0; i < itemCount; i++) {
            valueArray[i] = maxValue;
        }

        EquivalentCompressWriter<T> writer;
        writer.Init(slotItemCount);

        auto [status, compressLength] = writer.CalculateCompressLength(valueArray, slotItemCount);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(valueArray);
        writer.DumpBuffer(buffer, compressLength);

        EquivalentCompressReader<T> compArray;
        indexlib::util::SimpleMemoryQuotaControllerPtr controller(new indexlib::util::SimpleMemoryQuotaController(
            util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        indexlib::util::BytesAlignedSliceArrayPtr expandSliceArray(
            new indexlib::util::BytesAlignedSliceArray(controller, 64 * 1024 * 1024, 10));
        compArray.Init(buffer, compressLength, expandSliceArray);

        size_t pos = 0;
        int64_t beginTime = autil::TimeUtility::currentTime();
        std::cout << "***** begin expand update !******" << std::endl;
        // compress expand update
        for (size_t i = 0; i < updateTimes; i++) {
            ASSERT_TRUE(compArray.Update(pos, --maxValue));
            pos = (pos + updateStep) % itemCount;
        }
        int64_t interval = autil::TimeUtility::currentTime() - beginTime;

        double updateQps = updateTimes / ((double)interval / 1000 / 1000);
        std::cout << "***** time interval : " << interval / 1000 << "ms, QPS:" << (int64_t)updateQps << std::endl;
        std::cout << "############################" << std::endl;
        delete[] buffer;
    }

    template <typename T>
    void InnerTestRandomUpdate()
    {
        std::cout << "##########" << std::endl;
        std::vector<T> array;
        MakeData<T>(20000000, array, 95);

        std::vector<T> updateArray;
        MakeData<T>(20000000, updateArray, 97);

        EquivalentCompressWriter<T> writer;
        writer.Init(1024);
        auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);

        indexlib::util::SimpleMemoryQuotaControllerPtr controller(new indexlib::util::SimpleMemoryQuotaController(
            util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        indexlib::util::BytesAlignedSliceArrayPtr expandSliceArray(
            new indexlib::util::BytesAlignedSliceArray(controller, indexlib::index::EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                                                       indexlib::index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));

        std::cout << "**********" << std::endl;
        EquivalentCompressReader<T> compArray;
        compArray.Init(buffer, compressLength, expandSliceArray);
        for (size_t i = 0; i < array.size(); i++) {
            ASSERT_TRUE(compArray.Update((docid_t)i, updateArray[i]));
            auto [status, curVal] = compArray[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(curVal, updateArray[i]);
        }

        for (size_t i = 0; i < array.size(); i++) {
            auto [status, curVal] = compArray[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(curVal, updateArray[i]);
        }
        delete[] buffer;
    }

private:
    indexlib::util::BlockMemoryQuotaControllerPtr _memoryController;
};

void EquivalentCompressReaderPerfTest::setUp()
{
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

TEST_F(EquivalentCompressReaderPerfTest, TestReadLongCaseTest)
{
    {
        uint32_t value[] = {17, 2, 3, 9, 17, 5, 23, 68, 281, 321, 3, 647, 823, 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        auto [status, compressLength] = writer.CalculateCompressLength(value, 20, 8);
        ASSERT_TRUE(status.IsOK());

        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<uint32_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        {
            FileNodePtr fileNode(new MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
            string path = "in_mem_file";
            ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
            fileNode->Write(buffer, compressLength).GetOrThrow();
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        delete[] buffer;
    }

    {
        uint64_t value[] = {17, 2, 3, 9, 17, 5, 23, 68, 281, 321, 3, 647, ((uint64_t)1 << 63), 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);
        auto [status, compressLength] = writer.CalculateCompressLength(value, 20, 8);
        ASSERT_TRUE(status.IsOK());

        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<uint64_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        {
            FileNodePtr fileNode(new MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
            string path = "in_mem_file";
            ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
            fileNode->Write(buffer, compressLength).GetOrThrow();
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        delete[] buffer;
    }

    {
        int32_t value[] = {-17, 2, 3, 9, 17, 5, 23, 68, 281, 321, 3, 647, -823, 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<int32_t> writer;
        writer.Init(8);
        auto [status, compressLength] = writer.CalculateCompressLength(value, 20, 8);
        ASSERT_TRUE(status.IsOK());

        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<int32_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        {
            FileNodePtr fileNode(new MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
            string path = "in_mem_file";
            ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
            fileNode->Write(buffer, compressLength).GetOrThrow();
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<int32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        delete[] buffer;
    }

    {
        int64_t value[] = {17, 2, 3, 9, 17, 5, 23, 68, 281, 321, 3, -647, -((int64_t)1 << 60), 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        auto [status, compressLength] = writer.CalculateCompressLength(value, 20, 8);
        ASSERT_TRUE(status.IsOK());

        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<int64_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        {
            FileNodePtr fileNode(new MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
            string path = "in_mem_file";
            ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
            fileNode->Write(buffer, compressLength).GetOrThrow();
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<int64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[i], curVal);
            }
        }
        delete[] buffer;
    }

    // mass random data
    {
        std::vector<uint32_t> array;
        MakeData<uint32_t>(20000000, array, 97);

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(1024);
        auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<uint32_t> compArray(buffer);
            for (size_t i = 0; i < array.size(); i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(array[i], curVal);
            }
        }
        {
            FileNodePtr fileNode(new MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
            string path = "in_mem_file";
            ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
            fileNode->Write(buffer, compressLength).GetOrThrow();
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(array[i], curVal);
            }
        }
        delete[] buffer;
    }
    {
        std::vector<uint64_t> array;
        MakeData<uint64_t>(20000000, array, 97);
        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(1024);
        auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        int64_t latency = 0;
        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<uint64_t> compArray(buffer);
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(array[i], curVal);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        latency = 0;
        {
            FileNodePtr fileNode(new MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
            string path = "in_mem_file";
            ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
            fileNode->Write(buffer, compressLength).GetOrThrow();
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++) {
                auto [status, curVal] = compArray.Get(i);
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(array[i], curVal);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        delete[] buffer;
    }
    {
        std::vector<int32_t> array;
        MakeData<int32_t>(20000000, array, 97);

        EquivalentCompressWriter<int32_t> writer;
        writer.Init(1024);
        auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);
        EquivalentCompressReader<int32_t> compArray(buffer);
        for (size_t i = 0; i < array.size(); i++) {
            auto [status, curVal] = compArray.Get(i);
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(array[i], curVal);
        }
        delete[] buffer;
    }

    {
        std::vector<int64_t> array;
        MakeData<int64_t>(20000000, array, 97);

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(1024);
        auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);
        EquivalentCompressReader<int64_t> compArray(buffer);
        for (size_t i = 0; i < array.size(); i++) {
            auto [status, curVal] = compArray.Get(i);
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(array[i], curVal);
        }
        delete[] buffer;
    }
}
TEST_F(EquivalentCompressReaderPerfTest, TestInplaceUpdatePerfLongCaseTest)
{
    InnerTestInplaceUpdatePerf<uint32_t>(1, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint32_t>(3, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint32_t>(15, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint32_t>(255, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint32_t>(65535, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint32_t>(100000, 5000000, 64, 1000000, 8);

    InnerTestInplaceUpdatePerf<uint32_t>(1, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint32_t>(3, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint32_t>(15, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint32_t>(255, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint32_t>(65535, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint32_t>(100000, 5000000, 64, 1000000, 64);

    InnerTestInplaceUpdatePerf<uint64_t>(1, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint64_t>(3, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint64_t>(15, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint64_t>(255, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint64_t>(65535, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint64_t>(100000, 5000000, 64, 1000000, 8);
    InnerTestInplaceUpdatePerf<uint64_t>((uint64_t)1 << 33, 5000000, 64, 1000000, 8);

    InnerTestInplaceUpdatePerf<uint64_t>(1, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint64_t>(3, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint64_t>(15, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint64_t>(255, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint64_t>(65535, 5000000, 64, 1000000, 64);
    InnerTestInplaceUpdatePerf<uint64_t>((uint64_t)1 << 33, 5000000, 64, 1000000, 64);
}
TEST_F(EquivalentCompressReaderPerfTest, TestExpandUpdatePerf)
{
    InnerTestExpandUpdatePerf<uint32_t>(5000000, 64, 8);
    InnerTestExpandUpdatePerf<uint32_t>(5000000, 64, 64);

    InnerTestExpandUpdatePerf<uint32_t>(5000000, 1024, 8);
    InnerTestExpandUpdatePerf<uint32_t>(5000000, 1024, 64);

    InnerTestExpandUpdatePerf<uint64_t>(5000000, 64, 8);
    InnerTestExpandUpdatePerf<uint64_t>(5000000, 64, 64);

    InnerTestExpandUpdatePerf<uint64_t>(5000000, 1024, 8);
    InnerTestExpandUpdatePerf<uint64_t>(5000000, 1024, 64);
}
TEST_F(EquivalentCompressReaderPerfTest, TestRandomUpdateLongCaseTest)
{
    InnerTestRandomUpdate<uint32_t>();
    InnerTestRandomUpdate<int32_t>();
    InnerTestRandomUpdate<uint64_t>();
    InnerTestRandomUpdate<int64_t>();
}
} // namespace indexlib::index
