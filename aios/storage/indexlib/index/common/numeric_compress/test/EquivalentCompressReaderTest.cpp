#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"

#include <random>

#include "autil/TimeUtility.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressWriter.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib::index {

class EquivalentCompressReaderTest : public INDEXLIB_TESTBASE
{
public:
    EquivalentCompressReaderTest();
    ~EquivalentCompressReaderTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    void DoMultiThreadTest(size_t readThreadNum, int64_t duration)
    {
        std::vector<int> status(readThreadNum, 0);
        _isFinish = false;
        _isRun = false;

        std::vector<autil::ThreadPtr> readThreads;
        for (size_t i = 0; i < readThreadNum; ++i) {
            autil::ThreadPtr thread =
                autil::Thread::createThread(std::bind(&EquivalentCompressReaderTest::Read, this, &status[i]));
            readThreads.push_back(thread);
        }
        autil::ThreadPtr writeThread =
            autil::Thread::createThread(std::bind(&EquivalentCompressReaderTest::Write, this));

        _isRun = true;
        int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
        int64_t endTime = beginTime;
        while (endTime - beginTime < duration) {
            sleep(1);
            endTime = autil::TimeUtility::currentTimeInSeconds();
        }
        _isFinish = true;

        for (size_t i = 0; i < readThreadNum; ++i) {
            readThreads[i].reset();
        }
        writeThread.reset();
        for (size_t i = 0; i < readThreadNum; ++i) {
            ASSERT_EQ((int)0, status[i]);
        }
    }

    void Write()
    {
        while (!_isRun) {
            usleep(0);
        }
        DoWrite();
    }
    void Read(int* status)
    {
        while (!_isRun) {
            usleep(0);
        }
        DoRead(status);
    }

    bool IsFinished() { return _isFinish; }

    template <typename T>
    void InnerTestInplaceUpdate(T initValue, T updateValue)
    {
        const size_t COUNT = 128;
        for (size_t i = 64; i < COUNT; i++) {
            T value[COUNT] = {0};
            value[i] = initValue;

            EquivalentCompressWriter<T> writer;
            writer.Init(64);

            auto [status, compressLength] = writer.CalculateCompressLength(value, COUNT, 64);
            ASSERT_TRUE(status.IsOK());
            uint8_t* buffer = new uint8_t[compressLength];
            writer.CompressData(value, COUNT);
            writer.DumpBuffer(buffer, compressLength);

            EquivalentCompressReader<T> compArray(buffer);
            for (size_t j = 0; j < COUNT; j++) {
                auto [status, curVal] = compArray[j];
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[j], curVal);
            }

            ASSERT_TRUE(compArray.Update(0, 0));
            ASSERT_TRUE(compArray.Update(i, updateValue));

            value[i] = updateValue;
            for (size_t j = 0; j < 128; j++) {
                auto [status, curVal] = compArray[j];
                ASSERT_TRUE(status.IsOK());
                ASSERT_EQ(value[j], curVal);
            }
            delete[] buffer;
        }
    }

    template <typename T>
    void InnerTestInplaceUpdate(T initValue, const std::string& updateValueStrs)
    {
        std::vector<T> updateValues;
        autil::StringUtil::fromString(updateValueStrs, updateValues, ",");
        for (size_t i = 0; i < updateValues.size(); i++) {
            InnerTestInplaceUpdate(initValue, updateValues[i]);
        }
    }

    template <typename T>
    void InnerTestExpandUpdate(uint32_t itemCount, T equalValue, size_t initPos, T initValue,
                               const std::vector<size_t>& updatePosVec, const std::vector<T>& updateValueVec,
                               size_t expectExpandSize, bool checkUpdateMetrics = false,
                               size_t expectNoUseMemorySize = 0, uint32_t expectExpandUpdateCount = 0)
    {
        std::vector<T> values;
        values.resize(itemCount, equalValue);

        values[initPos] = initValue;
        EquivalentCompressWriter<T> writer;
        writer.Init(64);

        auto [status, compressLength] = writer.CalculateCompressLength(values, 64);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(values);
        writer.DumpBuffer(buffer, compressLength);

        indexlib::util::SimpleMemoryQuotaControllerPtr controller(new indexlib::util::SimpleMemoryQuotaController(
            indexlib::util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        indexlib::util::BytesAlignedSliceArrayPtr expandSliceArray(
            new indexlib::util::BytesAlignedSliceArray(controller, 1024 * 1024));
        EquivalentCompressReader<T> compArray;
        compArray.Init(buffer, compressLength, expandSliceArray);

        for (size_t j = 0; j < itemCount; j++) {
            auto [status, curVal] = compArray[j];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(values[j], curVal);
        }

        assert(updatePosVec.size() == updateValueVec.size());
        uint32_t validUpdateCount = 0;
        for (size_t i = 0; i < updatePosVec.size(); i++) {
            if (updatePosVec[i] >= itemCount) {
                ASSERT_FALSE(compArray.Update(updatePosVec[i], updateValueVec[i]));
            } else {
                ASSERT_TRUE(compArray.Update(updatePosVec[i], updateValueVec[i]));
                validUpdateCount++;
                values[updatePosVec[i]] = updateValueVec[i];
            }
        }

        if (checkUpdateMetrics) {
            EquivalentCompressUpdateMetrics updateMetrics = compArray.GetUpdateMetrics();
            ASSERT_EQ(expectNoUseMemorySize, updateMetrics.noUsedBytesSize);
            ASSERT_EQ(expectExpandUpdateCount, updateMetrics.expandUpdateCount);
            ASSERT_EQ(validUpdateCount - expectExpandUpdateCount, updateMetrics.inplaceUpdateCount);
        }

        ASSERT_EQ(expectExpandSize, expandSliceArray->GetTotalUsedBytes());
        for (size_t j = 0; j < itemCount; j++) {
            auto [status, curVal] = compArray[j];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(values[j], curVal);
        }
        delete[] buffer;
    }

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
    void InnerTestBatchGet(bool testEqual)
    {
        {
            std::vector<T> array;
            if (testEqual) {
                MakeData<T>(100000, array, 101);
            } else {
                MakeData<T>(100000, array, 0);
            }

            EquivalentCompressWriter<T> writer;
            writer.Init(1024);
            auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
            ASSERT_TRUE(status.IsOK());
            uint8_t* buffer = new uint8_t[compressLength];
            writer.CompressData(array);
            size_t compSize = writer.DumpBuffer(buffer, compressLength);

            ASSERT_EQ(compSize, compressLength);
            autil::mem_pool::Pool pool;
            {
                // init from buffer
                EquivalentCompressReader<T> compArray(buffer);
                EquivalentCompressSessionOption option;
                auto sessionReader = compArray.CreateSessionReader(&pool, option);
                size_t count = array.size() < 10 ? array.size() : 10;
                std::vector<int32_t> batchPos(count);
                for (size_t i = 0; i < batchPos.size(); ++i) {
                    batchPos[i] = i;
                }
                std::vector<T> values;
                auto getResult =
                    future_lite::coro::syncAwait(sessionReader.BatchGet(batchPos, file_system::ReadOption(), &values));
                ASSERT_EQ(count, getResult.size());
                ASSERT_EQ(count, values.size());
                for (size_t i = 0; i < count; ++i) {
                    ASSERT_EQ(indexlib::index::ErrorCode::OK, getResult[i]);
                    ASSERT_EQ(array[i], values[i]);
                }
            }

            {
                // init from file reader, will call read from fileReader
                FileNodePtr fileNode(
                    new file_system::MemFileNode((size_t)compressLength, false, LoadConfig(), _memoryController));
                std::string path = "in_mem_file";
                ASSERT_EQ(FSEC_OK, fileNode->Open("LOGICAL_PATH", path, FSOT_MEM, -1));
                fileNode->Write(buffer, compressLength).GetOrThrow();
                NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
                EquivalentCompressReader<T> compArray;
                ASSERT_TRUE(compArray.Init(fileReader));
                auto checkFunction = [&](EquivalentCompressSessionReader<T>& reader, std::vector<int32_t> pos) {
                    std::vector<T> values;
                    auto getResult =
                        future_lite::coro::syncAwait(reader.BatchGet(pos, file_system::ReadOption(), &values));
                    ASSERT_EQ(pos.size(), getResult.size());
                    ASSERT_EQ(pos.size(), values.size());
                    for (size_t i = 0; i < pos.size(); ++i) {
                        ASSERT_EQ(indexlib::index::ErrorCode::OK, getResult[i]);
                        ASSERT_EQ(array[pos[i]], values[i]);
                    }
                };
                {
                    // normal check
                    EquivalentCompressSessionReader<T> sessionReader =
                        compArray.CreateSessionReader(nullptr, EquivalentCompressSessionOption());
                    checkFunction(sessionReader, {1, 2, 3, 4, 6, 7});
                    checkFunction(sessionReader, {3, 3, 3, 3});
                    checkFunction(sessionReader, {});
                }
                {
                    // read back
                    EquivalentCompressSessionReader<T> sessionReader =
                        compArray.CreateSessionReader(nullptr, EquivalentCompressSessionOption());
                    checkFunction(sessionReader, {1, 2, 8, 100});
                    checkFunction(sessionReader, {100, 200, 300});
                    checkFunction(sessionReader, {3, 4, 5});
                }
                {
                    EquivalentCompressSessionReader<T> sessionReader =
                        compArray.CreateSessionReader(nullptr, EquivalentCompressSessionOption());
                    std::vector<T> values;
                    // unordered, will read fail
                    ASSERT_EQ(indexlib::index::ErrorCodeVec(
                                  {indexlib::index::ErrorCode::BadParameter, indexlib::index::ErrorCode::BadParameter}),
                              future_lite::coro::syncAwait(
                                  sessionReader.BatchGet({2, 1}, file_system::ReadOption(), &values)));

                    // read over bound, will read fail
                    ASSERT_EQ(indexlib::index::ErrorCodeVec(
                                  {indexlib::index::ErrorCode::BadParameter, indexlib::index::ErrorCode::BadParameter}),
                              future_lite::coro::syncAwait(
                                  sessionReader.BatchGet({2, 100000 + 100}, file_system::ReadOption(), &values)));
                }
            }
            delete[] buffer;
        }
    }

    template <typename T>
    void InnerTestExpandUpdateFromEqualSlot()
    {
        std::vector<size_t> updateIdxs;
        std::vector<T> updateValues;

        updateIdxs.push_back(0);
        updateValues.push_back(100);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte store noUseBytes size
        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        /****** not last slot ********/
        // from equal to expand new slot data
        InnerTestExpandUpdate(100, (T)0, 0, (T)0, updateIdxs, updateValues, expandSize, true, 0, 1);
        InnerTestExpandUpdate(100, (T)200, 0, (T)200, updateIdxs, updateValues, expandSize, true, 0, 1);

        updateIdxs.push_back(1);
        updateValues.push_back(130);

        // inplace update after first expand
        InnerTestExpandUpdate(100, (T)0, 0, (T)0, updateIdxs, updateValues, expandSize, true, 0, 1);
        InnerTestExpandUpdate(100, (T)200, 0, (T)200, updateIdxs, updateValues, expandSize, true, 0, 1);

        /******  last slot ********/
        updateIdxs.push_back(64);
        updateValues.push_back(100);

        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        // from equal to expand new slot data
        InnerTestExpandUpdate(100, (T)0, 0, (T)0, updateIdxs, updateValues, expandSize);
        InnerTestExpandUpdate(100, (T)200, 0, (T)200, updateIdxs, updateValues, expandSize);

        // idx out of range, update fail
        updateIdxs.push_back(100);
        updateValues.push_back(100);

        InnerTestExpandUpdate(100, (T)0, 0, (T)0, updateIdxs, updateValues, expandSize);
        InnerTestExpandUpdate(100, (T)200, 0, (T)200, updateIdxs, updateValues, expandSize);
    }

    template <typename T>
    void InnerTestExpandUpdateForSmallerThanBaseValue()
    {
        std::vector<size_t> updateIdxs;
        std::vector<T> updateValues;

        updateIdxs.push_back(2);
        updateValues.push_back(100);

        /****** not last slot ******/
        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte store noUseBytes size
        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        // baseValue 200 -> 100, maxDelta = 200

        size_t wasteMemorySize = GetExpandSize<T>(64, SIT_DELTA_UINT8);
        InnerTestExpandUpdate(100, (T)300, 0, (T)200, updateIdxs, updateValues, expandSize, true, wasteMemorySize, 1);

        updateIdxs.push_back(3);
        updateValues.push_back(0);

        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT16);
        wasteMemorySize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        // baseValue 100 -> 0, maxDelta = 300
        InnerTestExpandUpdate(100, (T)300, 0, (T)200, updateIdxs, updateValues, expandSize, true, wasteMemorySize, 2);

        /****** last slot ******/
        updateIdxs.clear();
        updateValues.clear();

        updateIdxs.push_back(65);
        updateValues.push_back(100);
        expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte store noUseBytes size
        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT8);

        wasteMemorySize = GetExpandSize<T>(36, SIT_DELTA_UINT8);
        InnerTestExpandUpdate(100, (T)300, 64, (T)200, updateIdxs, updateValues, expandSize, true, wasteMemorySize, 1);

        updateIdxs.push_back(66);
        updateValues.push_back(0);
        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT16);
        wasteMemorySize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        InnerTestExpandUpdate(100, (T)300, 64, (T)200, updateIdxs, updateValues, expandSize, true, wasteMemorySize, 2);
    }

    template <typename T>
    void InnerTestExpandUpdateForExpandStoreSize(size_t expandCount)
    {
        std::vector<size_t> updateIdxs;
        updateIdxs.push_back(1);
        updateIdxs.push_back(2);
        updateIdxs.push_back(3);
        updateIdxs.push_back(4);
        updateIdxs.push_back(5);
        updateIdxs.push_back(6);

        std::vector<T> updateValues;
        updateValues.push_back(2);              // 2bit
        updateValues.push_back(4);              // 4bit
        updateValues.push_back(16);             // 1 byte
        updateValues.push_back(256);            // 2 bytes
        updateValues.push_back(65536);          // 4 bytes
        updateValues.push_back((T)0x100000000); // 8 bytes

        std::vector<size_t> expandSizes;
        expandSizes.push_back(GetExpandSize<T>(64, SIT_DELTA_BIT2));
        expandSizes.push_back(GetExpandSize<T>(64, SIT_DELTA_BIT4));
        expandSizes.push_back(GetExpandSize<T>(64, SIT_DELTA_UINT8));
        expandSizes.push_back(GetExpandSize<T>(64, SIT_DELTA_UINT16));
        expandSizes.push_back(GetExpandSize<T>(64, SIT_DELTA_UINT32));
        expandSizes.push_back(GetExpandSize<T>(64, SIT_DELTA_UINT64));

        std::vector<size_t> idxs;
        std::vector<T> values;

        // first 16 byte store noUseBytes
        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics);
        for (size_t i = 0; i < expandCount; i++) {
            idxs.push_back(updateIdxs[i]);
            values.push_back(updateValues[i]);
            expandSize += expandSizes[i];
            InnerTestExpandUpdate(100, (T)0, 0, (T)1, idxs, values, expandSize);
        }

        for (size_t i = 0; i < expandCount; i++) {
            std::vector<size_t> idxs;
            std::vector<T> values;
            size_t expandSize = 0;

            idxs.push_back(updateIdxs[i]);
            values.push_back(updateValues[i]);
            expandSize = sizeof(EquivalentCompressUpdateMetrics) + expandSizes[i];
            InnerTestExpandUpdate(100, (T)0, 0, (T)1, idxs, values, expandSize);
        }
    }

    template <typename T>
    size_t GetExpandSize(uint32_t slotItemCount, SlotItemType slotType)
    {
        size_t expandSize = 0;
        if (sizeof(T) == (size_t)8) {
            expandSize = (8 * 2);
        } else {
            expandSize = sizeof(T);
        }

        expandSize += GetCompressDeltaBufferSize(slotType, slotItemCount);
        return expandSize;
    }

    template <typename T>
    void InnerTestReadLegacyData()
    {
        const int dataCount = 1024;
        const int itemPerBlock = 64;
        std::vector<T> originData;
        for (int i = 0; i < dataCount; i++) {
            originData.push_back((T)0);
        }

        uint64_t deltaArray[] = {128, 20000, 100000, 5000000000};
        uint32_t deltaCount = (sizeof(T) == sizeof(uint64_t)) ? 4 : 3;
        for (uint32_t i = 0; i < deltaCount; i++) {
            originData[i * itemPerBlock] = deltaArray[i];
        }

        std::string fileName = (sizeof(T) == sizeof(uint64_t)) ? "u64_data" : "u32_data";
        std::string filePath = util::PathUtil::JoinPath(_legacyDataPath, fileName);

        std::string fileContent;
        file_system::FslibWrapper::AtomicLoadE(filePath, fileContent);

        uint8_t* buffer = (uint8_t*)fileContent.data();
        EquivalentCompressReader<T> compReader(buffer);

        ASSERT_EQ(originData.size(), compReader.Size());
        for (size_t i = 0; i < originData.size(); i++) {
            auto [status, curVal] = compReader[i];
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(originData[i], curVal);
        }
    }

    template <typename T>
    std::pair<uint8_t*, size_t> MakeBuffer()
    {
        std::vector<T> values;
        values.resize(_total, std::numeric_limits<T>::max() / 2);
        EquivalentCompressWriter<T> writer;
        writer.Init(64);
        auto [status, compressLength] = writer.CalculateCompressLength(values, 64);
        if (!status.IsOK()) {
            return {nullptr, 0};
        }
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(values);
        writer.DumpBuffer(buffer, compressLength);
        return {buffer, compressLength};
    }

    template <typename T>
    std::pair<uint8_t*, size_t> MakeBufferByRule()
    {
        std::vector<T> values;
        values.resize(_total, std::numeric_limits<T>::max() / 2);

        for (size_t i = 0; i < _total; i++) {
            if (i % 2 == 0) {
                values[i] = _seed;
            }
        }
        EquivalentCompressWriter<T> writer;
        writer.Init(64);
        auto [status, compressLength] = writer.CalculateCompressLength(values, 64);
        if (!status.IsOK()) {
            return {nullptr, 0};
        }
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(values);
        writer.DumpBuffer(buffer, compressLength);
        return {buffer, compressLength};
    }

    template <typename T>
    void DoUpdate(size_t pos, EquivalentCompressReader<T>& reader)
    {
        if (_needUpdate) {
            auto updatedValue = std::numeric_limits<T>::max() / 4 * 3;
            ASSERT_TRUE(reader.Update(_pos, updatedValue));
        }
    }

    template <typename T>
    void DoCheck(size_t pos, EquivalentCompressReader<T>& reader)
    {
        auto [status, value] = reader[pos];
        if (!status.IsOK()) {
            std::abort();
        }
        if (_oddEvenCheck) {
            if (pos % 2 == 0) {
                if (value != _seed) {
                    std::abort();
                }
            } else {
                if (value != std::numeric_limits<T>::max() / 2) {
                    std::abort();
                }
            }
        } else {
            if (value != std::numeric_limits<T>::max() / 2 && value != std::numeric_limits<T>::max() / 4 * 3) {
                std::cerr << "actual: " << value << " , "
                          << " max: " << std::numeric_limits<T>::max() << std::endl;
                std::abort();
            }
        }
    }

    void DoWrite();
    void DoRead(int* status);

private:
    std::string _legacyDataPath;

    // for multithread test
    std::mt19937 _random;
    const size_t _total;
    volatile size_t _pos;
    uint8_t* _u64Buffer;
    uint8_t* _u32Buffer;
    EquivalentCompressReader<uint64_t> _u64Reader;
    EquivalentCompressReader<uint32_t> _u32Reader;
    std::shared_ptr<indexlib::util::BlockMemoryQuotaController> _memoryController;
    bool _needUpdate;
    bool _seed;
    bool _oddEvenCheck;
    bool volatile _isFinish;
    bool volatile _isRun;
};

EquivalentCompressReaderTest::EquivalentCompressReaderTest()
    : _random(0)
    , _total(10000)
    , _pos(0)
    , _u64Buffer(NULL)
    , _u32Buffer(NULL)
    , _needUpdate(true)
    , _seed(0u)
    , _oddEvenCheck(false)
{
}

EquivalentCompressReaderTest::~EquivalentCompressReaderTest() {}

void EquivalentCompressReaderTest::CaseSetUp()
{
    _legacyDataPath = util::PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "legacy_equal_compress_data");
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void EquivalentCompressReaderTest::CaseTearDown()
{
    if (_u64Buffer) {
        delete[] _u64Buffer;
        _u64Buffer = NULL;
    }
    if (_u32Buffer) {
        delete[] _u32Buffer;
        _u32Buffer = NULL;
    }
}

TEST_F(EquivalentCompressReaderTest, TestInit) {{// slotItemNum is 128, totalItemCount 0
                                                 uint32_t buffer[] = {0, 7};
EquivalentCompressReaderBase array((const uint8_t*)buffer,
                                   /*comLen*/ EquivalentCompressReaderBase::INVALID_COMPRESS_LEN);

ASSERT_EQ((uint32_t)0, array._itemCount);
ASSERT_EQ((uint32_t)7, array._slotBitNum);
ASSERT_EQ((uint32_t)127, array._slotMask);
ASSERT_EQ((uint32_t)0, array._slotNum);
ASSERT_TRUE(array._slotBaseAddr == NULL);
ASSERT_TRUE(array._valueBaseAddr == NULL);
} // namespace indexlib::index
{
    // slotItemNum is 128, totalItemCount 3
    uint32_t buffer[] = {3, 7};
    EquivalentCompressReader<uint32_t> array((const uint8_t*)buffer);

    ASSERT_EQ((uint32_t)3, array._itemCount);
    ASSERT_EQ((uint32_t)7, array._slotBitNum);
    ASSERT_EQ((uint32_t)127, array._slotMask);
    ASSERT_EQ((uint32_t)1, array._slotNum);

    // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
    ASSERT_EQ((uint64_t*)((uint8_t*)buffer + sizeof(buffer)), array._slotBaseAddr);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
    // + slotItemNum * slotItemSize : sizeof(uint64_t)
    ASSERT_EQ((uint8_t*)buffer + sizeof(buffer) + sizeof(uint64_t), array._valueBaseAddr);
#pragma GCC diagnostic pop
}
{
    // slotItemNum is 128, totalItemCount 256
    uint32_t buffer[] = {256, 7};
    EquivalentCompressReader<uint64_t> array((const uint8_t*)buffer);

    ASSERT_EQ((uint32_t)256, array._itemCount);
    ASSERT_EQ((uint32_t)7, array._slotBitNum);
    ASSERT_EQ((uint32_t)127, array._slotMask);
    ASSERT_EQ((uint32_t)2, array._slotNum);

    // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
    ASSERT_EQ((uint64_t*)((uint8_t*)buffer + sizeof(buffer)), array._slotBaseAddr);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
    // + slotItemNum * slotItemSize : sizeof(uint64_t)
    ASSERT_EQ((uint8_t*)buffer + sizeof(buffer) + 2 * sizeof(uint64_t), array._valueBaseAddr);
}
#pragma GCC diagnostic pop
}

TEST_F(EquivalentCompressReaderTest, TestReadWithSessionReader)
{
    {
        vector<uint64_t> array;
        MakeData<uint64_t>(10000000, array, 97);
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
                auto [status, curVal] = compArray[i];
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
            EquivalentCompressSessionReader<uint64_t> sessionReader =
                compArray.CreateSessionReader(nullptr, EquivalentCompressSessionOption());
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++) {
                auto [st, actualVal] = sessionReader.Get(i);
                ASSERT_TRUE(st.IsOK());
                ASSERT_EQ(array[i], actualVal);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        delete[] buffer;
    }
    {
        vector<int32_t> array;
        MakeData<int32_t>(10000000, array, 97);
        EquivalentCompressWriter<int32_t> writer;
        writer.Init(1024);
        auto [status, compressLength] = writer.CalculateCompressLength(array, 1024);
        ASSERT_TRUE(status.IsOK());
        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        int64_t latency = 0;
        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<int32_t> compArray(buffer);
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++) {
                auto [status, curVal] = compArray[i];
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
            EquivalentCompressReader<int32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            EquivalentCompressSessionReader<int32_t> sessionReader =
                compArray.CreateSessionReader(nullptr, EquivalentCompressSessionOption());
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++) {
                auto [st, actualVal] = sessionReader.Get(i);
                ASSERT_TRUE(st.IsOK());
                ASSERT_EQ(array[i], actualVal);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        delete[] buffer;
    }
    {
        double value[] = {17, 2, 3, 9, 17, 5, 23, 68, 281, 321, 3, 647, 823, 31, 478, 83, 3, 3, 3, 3};

        EquivalentCompressWriter<double> writer;
        writer.Init(8);
        auto [status, compressLength] = writer.CalculateCompressLength(value, 20, 8);
        ASSERT_TRUE(status.IsOK());

        uint8_t* buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<double> compArray(buffer);
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
            EquivalentCompressReader<double> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            auto sessionReader = compArray.CreateSessionReader(nullptr, EquivalentCompressSessionOption());
            for (size_t i = 0; i < 20; i++) {
                auto [st, actualVal] = sessionReader.Get(i);
                ASSERT_TRUE(st.IsOK());
                ASSERT_EQ(value[i], actualVal);
            }
        }
        delete[] buffer;
    }
}

TEST_F(EquivalentCompressReaderTest, TestBatchGet)
{
    InnerTestBatchGet<uint32_t>(true);
    InnerTestBatchGet<uint64_t>(true);
    InnerTestBatchGet<uint32_t>(false);
    InnerTestBatchGet<uint64_t>(false);
}

TEST_F(EquivalentCompressReaderTest, TestMultiThreadReadWithFileReader)
{
    _pos = 0;
    util::SimpleMemoryQuotaControllerPtr controller(
        new util::SimpleMemoryQuotaController(util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));

    _seed = 3;
    _oddEvenCheck = true;

    auto u64BufferInfo = MakeBufferByRule<uint64_t>();
    ASSERT_TRUE(u64BufferInfo.first != nullptr);
    _u64Buffer = u64BufferInfo.first;

    auto u32BufferInfo = MakeBufferByRule<uint32_t>();
    ASSERT_TRUE(u32BufferInfo.first != nullptr);
    _u32Buffer = u32BufferInfo.first;

    FileNodePtr fileNode1(new MemFileNode((size_t)u64BufferInfo.second, false, LoadConfig(), _memoryController));
    FileNodePtr fileNode2(new MemFileNode((size_t)u32BufferInfo.second, false, LoadConfig(), _memoryController));

    string path1 = "in_mem_file1";
    ASSERT_EQ(FSEC_OK, fileNode1->Open("LOGICAL_PATH", path1, FSOT_MEM, -1));
    fileNode1->Write(_u64Buffer, u64BufferInfo.second).GetOrThrow();

    string path2 = "in_mem_file2";
    ASSERT_EQ(FSEC_OK, fileNode2->Open("LOGICAL_PATH", path2, FSOT_MEM, -1));
    fileNode2->Write(_u32Buffer, u32BufferInfo.second).GetOrThrow();

    NormalFileReaderPtr fileReader1(new NormalFileReader(fileNode1));
    NormalFileReaderPtr fileReader2(new NormalFileReader(fileNode2));

    ASSERT_TRUE(_u64Reader.Init(fileReader1));
    ASSERT_TRUE(_u32Reader.Init(fileReader2));
    _needUpdate = false;
    DoMultiThreadTest(5, 10);
}

TEST_F(EquivalentCompressReaderTest, TestInplaceUpdate)
{
    InnerTestInplaceUpdate<uint32_t>(1, 0);        // 1bit update
    InnerTestInplaceUpdate<uint32_t>(2, "1,3");    // 2bit update
    InnerTestInplaceUpdate<uint32_t>(5, "1,3,10"); // 4bit update

    InnerTestInplaceUpdate<uint32_t>(20, "1,3,10,30");              // 1byte update
    InnerTestInplaceUpdate<uint32_t>(300, "1,3,10,30,500");         // 2byte update
    InnerTestInplaceUpdate<uint32_t>(70000, "1,3,10,30,500,77777"); // 4byte update

    InnerTestInplaceUpdate<uint64_t>(1, 0);        // 1bit update
    InnerTestInplaceUpdate<uint64_t>(2, "1,3");    // 2bit update
    InnerTestInplaceUpdate<uint64_t>(5, "1,3,10"); // 4bit update

    InnerTestInplaceUpdate<uint64_t>(20, "1,3,10,30");              // 1byte update
    InnerTestInplaceUpdate<uint64_t>(300, "1,3,10,30,500");         // 2byte update
    InnerTestInplaceUpdate<uint64_t>(70000, "1,3,10,30,500,77777"); // 4byte update

    InnerTestInplaceUpdate<uint64_t>(((uint64_t)1 << 33), "1,3,10,30,500,77777"); // 8byte update
    InnerTestInplaceUpdate<uint64_t>(((uint64_t)1 << 33), ((uint64_t)1 << 43));   // 8byte update
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateFromEqualSlot)
{
    InnerTestExpandUpdateFromEqualSlot<uint32_t>();
    InnerTestExpandUpdateFromEqualSlot<uint64_t>();
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateForSmallerThanBaseValue)
{
    InnerTestExpandUpdateForSmallerThanBaseValue<uint32_t>();
    InnerTestExpandUpdateForSmallerThanBaseValue<uint64_t>();
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateForExpandStoreSize)
{
    InnerTestExpandUpdateForExpandStoreSize<uint32_t>(5);
    InnerTestExpandUpdateForExpandStoreSize<uint64_t>(6);
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateForExpandToMaxStoreType)
{
    {
        // T = uint32_t
        std::vector<size_t> updateIdxs;
        std::vector<uint32_t> updateValues;

        updateIdxs.push_back(2);
        uint32_t updateValue = (uint32_t)1000 + std::numeric_limits<uint16_t>::max();
        updateValues.push_back(updateValue);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte, store noUseBytes
        expandSize += sizeof(uint32_t) * 64;                         // delta
        expandSize += sizeof(uint32_t);                              // header

        // baseValue 200, maxDelta (4 byte)
        InnerTestExpandUpdate(100, (uint32_t)300, 0, (uint32_t)200, updateIdxs, updateValues, expandSize);

        // baseValue 300, maxDelta (4 byte)
        InnerTestExpandUpdate(100, (uint32_t)300, 0, (uint32_t)300, updateIdxs, updateValues, expandSize);

        updateIdxs.push_back(3);
        updateValues.push_back(1);
        // baseValue 0, inplace update
        InnerTestExpandUpdate(100, (uint32_t)300, 0, (uint32_t)200, updateIdxs, updateValues, expandSize);

        InnerTestExpandUpdate(100, (uint32_t)300, 0, (uint32_t)300, updateIdxs, updateValues, expandSize);
    }

    {
        // T = uint64_t
        std::vector<size_t> updateIdxs;
        std::vector<uint64_t> updateValues;

        updateIdxs.push_back(2);
        uint64_t updateValue = (uint64_t)1000 + std::numeric_limits<uint32_t>::max();
        updateValues.push_back(updateValue);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte, store noUseBytes
        expandSize += sizeof(uint64_t) * 64;
        expandSize += (sizeof(uint64_t) * 2);

        // baseValue 200, maxDelta (8 byte)
        InnerTestExpandUpdate(100, (uint64_t)300, 0, (uint64_t)200, updateIdxs, updateValues, expandSize);

        // baseValue 300, maxDelta (8 byte)
        InnerTestExpandUpdate(100, (uint64_t)300, 0, (uint64_t)300, updateIdxs, updateValues, expandSize);

        updateIdxs.push_back(3);
        updateValues.push_back(1);
        // baseValue 0, inplace update
        InnerTestExpandUpdate(100, (uint64_t)300, 0, (uint64_t)200, updateIdxs, updateValues, expandSize);
        InnerTestExpandUpdate(100, (uint64_t)300, 0, (uint64_t)300, updateIdxs, updateValues, expandSize);
    }
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateForExpandToZeroBaseValue)
{
    {
        // T = uint32_t
        std::vector<size_t> updateIdxs;
        std::vector<uint32_t> updateValues;

        updateIdxs.push_back(2);
        updateValues.push_back(100);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte, store noUseBytes
        expandSize += sizeof(uint32_t) * 64;                         // delta
        expandSize += sizeof(uint32_t);                              // header

        uint32_t updateValue = (uint32_t)1000 + std::numeric_limits<uint16_t>::max();
        // baseValue 300->100, maxDelta (4 byte)
        InnerTestExpandUpdate(100, (uint32_t)300, 0, updateValue, updateIdxs, updateValues, expandSize);
    }

    {
        // T = uint64_t
        std::vector<size_t> updateIdxs;
        std::vector<uint64_t> updateValues;

        updateIdxs.push_back(2);
        updateValues.push_back(100);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte, store noUseBytes
        expandSize += sizeof(uint64_t) * 64;
        expandSize += (sizeof(uint64_t) * 2);

        uint64_t updateValue = (uint64_t)1000 + std::numeric_limits<uint32_t>::max();
        // baseValue 300->100, maxDelta (8 byte)
        InnerTestExpandUpdate(100, (uint64_t)300, 0, updateValue, updateIdxs, updateValues, expandSize);
    }
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateFailWithoutExpandSliceArray)
{
    std::vector<uint32_t> values;
    values.resize(10, 0);

    EquivalentCompressWriter<uint32_t> writer;
    writer.Init(64);

    auto [status, compressLength] = writer.CalculateCompressLength(values, 64);
    ASSERT_TRUE(status.IsOK());
    uint8_t* buffer = new uint8_t[compressLength];
    writer.CompressData(values);
    writer.DumpBuffer(buffer, compressLength);

    EquivalentCompressReader<uint32_t> compArray;
    compArray.Init(buffer, /*comLen*/ EquivalentCompressReaderBase::INVALID_COMPRESS_LEN, nullptr);
    ASSERT_FALSE(compArray.Update(0, 1));
    delete[] buffer;
}

TEST_F(EquivalentCompressReaderTest, TestReadLegacyData)
{
    InnerTestReadLegacyData<uint32_t>();
    InnerTestReadLegacyData<uint64_t>();
}

TEST_F(EquivalentCompressReaderTest, TestExpandUpdateMultiThread)
{
    _pos = 0;
    util::SimpleMemoryQuotaControllerPtr controller(
        new util::SimpleMemoryQuotaController(util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));

    auto u64BufferInfo = MakeBuffer<uint64_t>();
    _u64Buffer = u64BufferInfo.first;
    ASSERT_TRUE(_u64Buffer != nullptr);

    auto u32BufferInfo = MakeBuffer<uint32_t>();
    _u32Buffer = u32BufferInfo.first;
    ASSERT_TRUE(_u32Buffer != nullptr);
    util::BytesAlignedSliceArrayPtr u64ExpandSliceArray(
        new util::BytesAlignedSliceArray(controller, indexlib::index::EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                                         indexlib::index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));
    util::BytesAlignedSliceArrayPtr u32ExpandSliceArray(
        new util::BytesAlignedSliceArray(controller, indexlib::index::EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                                         indexlib::index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));
    _u64Reader.Init(_u64Buffer, u64BufferInfo.second, u64ExpandSliceArray);
    _u32Reader.Init(_u32Buffer, u32BufferInfo.second, u32ExpandSliceArray);

    DoMultiThreadTest(5, 10);
}

void EquivalentCompressReaderTest::DoWrite()
{
    while (!IsFinished()) {
        _pos = _random() % _total;
        DoUpdate(_pos, _u64Reader);
        DoUpdate(_pos, _u32Reader);
    }
}

void EquivalentCompressReaderTest::DoRead(int* status)
{
    while (!IsFinished()) {
        auto pos = _pos;
        DoCheck(pos, _u64Reader);
        DoCheck(pos, _u32Reader);
    }
}
} // namespace indexlib::index
