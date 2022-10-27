#ifndef __INDEXLIB_EQUIVALENTCOMPRESSREADERTEST_H
#define __INDEXLIB_EQUIVALENTCOMPRESSREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/test/multi_thread_test_base.h"
#include <autil/TimeUtility.h>
#include <random>

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(common);

class EquivalentCompressReaderTest : public test::MultiThreadTestBase
{
public:
    EquivalentCompressReaderTest();
    ~EquivalentCompressReaderTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestInit();
    void TestReadLongCaseTest();
    void TestReadWithSessionReader();    
    void TestInplaceUpdate();
    void TestMultiThreadReadWithFileReader();
    void TestExpandUpdateFromEqualSlot();
    void TestExpandUpdateForSmallerThanBaseValue();
    void TestExpandUpdateForExpandStoreSize();
    void TestExpandUpdateForExpandToMaxStoreType();
    void TestExpandUpdateFailWithoutExpandSliceArray();
    void TestExpandUpdateForExpandToZeroBaseValue();

    void TestInplaceUpdatePerfLongCaseTest();
    void TestExpandUpdatePerf();
    void TestRandomUpdateLongCaseTest();

    void TestReadLegacyData();

    void TestExpandUpdateMultiThread();

private:
    template <typename T>
    void InnerTestInplaceUpdate(T initValue, T updateValue)
    {
        const size_t COUNT = 128;
        for (size_t i = 64; i < COUNT; i++)
        {
            T value[COUNT] = {0};
            value[i] = initValue;

            EquivalentCompressWriter<T> writer;
            writer.Init(64);

            size_t compressLength = writer.CalculateCompressLength(value, COUNT, 64);
            uint8_t *buffer = new uint8_t[compressLength];
            writer.CompressData(value, COUNT);
            writer.DumpBuffer(buffer, compressLength);

            EquivalentCompressReader<T> compArray(buffer);
            for (size_t j = 0; j < COUNT; j++)
            {
                ASSERT_EQ(value[j], compArray[j]);
            }

            ASSERT_TRUE(compArray.Update(0, 0));
            ASSERT_TRUE(compArray.Update(i, updateValue));

            value[i] = updateValue;
            for (size_t j = 0; j < 128; j++)
            {
                ASSERT_EQ(value[j], compArray[j]);
            }
            delete[] buffer;
        }
    }

    template <typename T>
    void InnerTestInplaceUpdate(T initValue, const std::string& updateValueStrs)
    {
        std::vector<T> updateValues;
        autil::StringUtil::fromString(updateValueStrs, updateValues, ",");
        for (size_t i = 0; i < updateValues.size(); i++)
        {
            InnerTestInplaceUpdate(initValue, updateValues[i]);
        }
    }

    template <typename T>
    void InnerTestExpandUpdate(uint32_t itemCount, T equalValue,
                               size_t initPos, T initValue,
                               const std::vector<size_t>& updatePosVec,
                               const std::vector<T>& updateValueVec,
                               size_t expectExpandSize,
                               bool checkUpdateMetrics = false,
                               size_t expectNoUseMemorySize = 0,
                               uint32_t expectExpandUpdateCount = 0)
    {
        std::vector<T> values;
        values.resize(itemCount, equalValue);

        values[initPos] = initValue;
        EquivalentCompressWriter<T> writer;
        writer.Init(64);

        size_t compressLength = writer.CalculateCompressLength(values, 64);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(values);
        writer.DumpBuffer(buffer, compressLength);

        util::SimpleMemoryQuotaControllerPtr controller(
                new util::SimpleMemoryQuotaController(
                        util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        util::BytesAlignedSliceArrayPtr expandSliceArray(
                new util::BytesAlignedSliceArray(controller, 1024 * 1024));
        EquivalentCompressReader<T> compArray;
        compArray.Init(buffer, compressLength, expandSliceArray);

        for (size_t j = 0; j < itemCount; j++)
        {
            ASSERT_EQ(values[j], compArray[j]);
        }

        assert(updatePosVec.size() == updateValueVec.size());
        uint32_t validUpdateCount = 0;
        for (size_t i = 0; i < updatePosVec.size(); i++)
        {
            if (updatePosVec[i] >= itemCount)
            {
                ASSERT_FALSE(compArray.Update(updatePosVec[i], updateValueVec[i]));
            }
            else
            {
                ASSERT_TRUE(compArray.Update(updatePosVec[i], updateValueVec[i]));
                validUpdateCount++;
                values[updatePosVec[i]] = updateValueVec[i];
            }
        }

        if (checkUpdateMetrics)
        {
            EquivalentCompressUpdateMetrics updateMetrics = 
                compArray.GetUpdateMetrics();
            ASSERT_EQ(expectNoUseMemorySize, updateMetrics.noUsedBytesSize);
            ASSERT_EQ(expectExpandUpdateCount, updateMetrics.expandUpdateCount);
            ASSERT_EQ(validUpdateCount - expectExpandUpdateCount, updateMetrics.inplaceUpdateCount);
        }

        ASSERT_EQ(expectExpandSize, expandSliceArray->GetTotalUsedBytes());
        for (size_t j = 0; j < itemCount; j++)
        {
            ASSERT_EQ(values[j], compArray[j]);
        }
        delete[] buffer;
    }

private:
    template <typename T>
    void MakeData(uint32_t count, std::vector<T> &valueArray, uint32_t ratio)
    {
        for(uint32_t i = 0; i < count; ++i)
        {
            T value = (T)rand();
            if ((uint32_t)(value % 100) < ratio)
            {
                valueArray.push_back((T)100);
            }
            else
            {
                valueArray.push_back(value);
            }
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
        InnerTestExpandUpdate(100, (T)300, 0, (T)200, updateIdxs, updateValues, expandSize, 
                              true, wasteMemorySize, 1);

        updateIdxs.push_back(3);
        updateValues.push_back(0);

        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT16);
        wasteMemorySize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        // baseValue 100 -> 0, maxDelta = 300
        InnerTestExpandUpdate(100, (T)300, 0, (T)200, updateIdxs, updateValues, expandSize,
                              true, wasteMemorySize, 2);

        /****** last slot ******/
        updateIdxs.clear();
        updateValues.clear();

        updateIdxs.push_back(65);
        updateValues.push_back(100);
        expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte store noUseBytes size
        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT8);

        wasteMemorySize = GetExpandSize<T>(36, SIT_DELTA_UINT8);
        InnerTestExpandUpdate(100, (T)300, 64, (T)200, updateIdxs, updateValues, expandSize,
                              true, wasteMemorySize, 1);

        updateIdxs.push_back(66);
        updateValues.push_back(0);
        expandSize += GetExpandSize<T>(64, SIT_DELTA_UINT16);
        wasteMemorySize += GetExpandSize<T>(64, SIT_DELTA_UINT8);
        InnerTestExpandUpdate(100, (T)300, 64, (T)200, updateIdxs, updateValues, expandSize,
                              true, wasteMemorySize, 2);
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
        updateValues.push_back(2);   // 2bit
        updateValues.push_back(4);   // 4bit   
        updateValues.push_back(16);  // 1 byte
        updateValues.push_back(256); // 2 bytes
        updateValues.push_back(65536); // 4 bytes
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
        for (size_t i = 0; i < expandCount; i++)
        {
            idxs.push_back(updateIdxs[i]);
            values.push_back(updateValues[i]);
            expandSize += expandSizes[i];
            InnerTestExpandUpdate(100, (T)0, 0, (T)1, idxs, values, expandSize);
        }

        for (size_t i = 0; i < expandCount; i++)
        {
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
    void InnerTestInplaceUpdatePerf(T value, uint32_t itemCount, uint32_t slotItemCount,
                                    uint32_t updateTimes, uint32_t updateStep)
    {
        std::vector<T> valueArray;
        valueArray.resize(itemCount);
        for (size_t i = 0; i < itemCount; i++)
        {
            valueArray[i] = (i % 2 == 0) ? 0 : value;
        }

        EquivalentCompressWriter<T> writer;
        writer.Init(slotItemCount);

        size_t compressLength = writer.CalculateCompressLength(valueArray, slotItemCount);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(valueArray);
        writer.DumpBuffer(buffer, compressLength);

        EquivalentCompressReader<T> compArray(buffer);
        size_t pos = 0;
        int64_t beginTime = autil::TimeUtility::currentTime();
        std::cout << "***** begin inplace update !******" << std::endl;

        // compress update
        for (size_t i = 0; i < updateTimes; i++)
        {
            ASSERT_TRUE(compArray.Update(pos, 0));
            pos = (pos + updateStep) % itemCount;
        }
        int64_t interval = autil::TimeUtility::currentTime() - beginTime;

        double updateQps = updateTimes / ((double)interval / 1000 / 1000);
        std::cout << "***** time interval : " << interval / 1000 
                  << "ms, QPS:" << (int64_t)updateQps << std::endl;

        beginTime = autil::TimeUtility::currentTime();
        std::cout << "***** begin array update !******" << std::endl;

        // array update
        for (size_t i = 0; i < updateTimes; i++)
        {
            valueArray[pos] = 0;
            pos = (pos + updateStep) % itemCount;
        }
        interval = autil::TimeUtility::currentTime() - beginTime;
        updateQps = updateTimes / ((double)interval / 1000 / 1000);
        std::cout << "***** time interval : " << interval / 1000 
                  << "ms, QPS:" << (int64_t)updateQps << std::endl;

        std::cout << "############################" << std::endl;
        delete[] buffer;
    }

    template <typename T>
    void InnerTestExpandUpdatePerf(uint32_t itemCount, 
                                   uint32_t slotItemCount, uint32_t updateStep)
    {
        assert(sizeof(T) == sizeof(uint32_t) || sizeof(T) == sizeof(uint64_t));

        T maxValue = std::numeric_limits<uint32_t>::max();
        size_t updateTimes = 65536; // ensure not expand reach to max slot type
        if (sizeof(T) == sizeof(uint64_t))
        {
            maxValue = (T)std::numeric_limits<uint64_t>::max();
        }

        std::vector<T> valueArray;
        valueArray.resize(itemCount);
        for (size_t i = 0; i < itemCount; i++)
        {
            valueArray[i] = maxValue;
        }

        EquivalentCompressWriter<T> writer;
        writer.Init(slotItemCount);

        size_t compressLength = writer.CalculateCompressLength(valueArray, slotItemCount);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(valueArray);
        writer.DumpBuffer(buffer, compressLength);

        EquivalentCompressReader<T> compArray;
        util::SimpleMemoryQuotaControllerPtr controller(
                new util::SimpleMemoryQuotaController(
                        util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        util::BytesAlignedSliceArrayPtr expandSliceArray(
                new util::BytesAlignedSliceArray(controller, 64 * 1024 * 1024, 10));
        compArray.Init(buffer, compressLength, expandSliceArray);
        
        size_t pos = 0;
        int64_t beginTime = autil::TimeUtility::currentTime();
        std::cout << "***** begin expand update !******" << std::endl;
        // compress expand update
        for (size_t i = 0; i < updateTimes; i++)
        {
            ASSERT_TRUE(compArray.Update(pos, --maxValue));
            pos = (pos + updateStep) % itemCount;
        }
        int64_t interval = autil::TimeUtility::currentTime() - beginTime;

        double updateQps = updateTimes / ((double)interval / 1000 / 1000);
        std::cout << "***** time interval : " << interval / 1000 
                  << "ms, QPS:" << (int64_t)updateQps << std::endl;
        std::cout << "############################" << std::endl;
        delete[] buffer;
    }

    template <typename T>
    size_t GetExpandSize(uint32_t slotItemCount, SlotItemType slotType)
    {
        size_t expandSize = 0;
        if (sizeof(T) == (size_t)8)
        {
            expandSize = (8 * 2);
        }
        else
        {
            expandSize = sizeof(T);
        }

        expandSize += GetCompressDeltaBufferSize(slotType, slotItemCount);
        return expandSize;
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
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);

        util::SimpleMemoryQuotaControllerPtr controller(
                new util::SimpleMemoryQuotaController(
                        util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));
        util::BytesAlignedSliceArrayPtr expandSliceArray(
                new util::BytesAlignedSliceArray(controller,
                        index::EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                        index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));


        std::cout << "**********" << std::endl;
        EquivalentCompressReader<T> compArray;
        compArray.Init(buffer, compressLength, expandSliceArray);
        for (size_t i = 0; i < array.size(); i++)
        {
            ASSERT_TRUE(compArray.Update((docid_t)i, updateArray[i]));
            ASSERT_EQ(compArray[i], updateArray[i]);
        }

        for (size_t i = 0; i < array.size(); i++)
        {
            ASSERT_EQ(compArray[i], updateArray[i]);
        }
        delete[] buffer;
    }

    template <typename T>
    void InnerTestReadLegacyData()
    {
        const int dataCount = 1024;
        const int itemPerBlock = 64;
        std::vector<T> originData;
        for (int i = 0; i < dataCount; i++)
        {
            originData.push_back((T)0);
        }
    
        uint64_t deltaArray[] = {128, 20000, 100000, 5000000000};
        uint32_t deltaCount = (sizeof(T) == sizeof(uint64_t)) ? 4 : 3;
        for (uint32_t i = 0; i < deltaCount; i++)
        {
            originData[i * itemPerBlock] = deltaArray[i];
        }

        std::string fileName = (sizeof(T) == sizeof(uint64_t)) ? "u64_data" : "u32_data";
        std::string filePath = storage::FileSystemWrapper::JoinPath(
                mLegacyDataPath, fileName);

        std::string fileContent;
        storage::FileSystemWrapper::AtomicLoad(filePath, fileContent);

        uint8_t* buffer = (uint8_t*)fileContent.data();
        EquivalentCompressReader<T> compReader(buffer);

        ASSERT_EQ(originData.size(), compReader.Size());
        for (size_t i = 0; i < originData.size(); i++)
        {
            ASSERT_EQ(originData[i], compReader[i]);
        }
    }

    template<typename T>
    std::pair<uint8_t*, size_t> MakeBuffer()
    {
        std::vector<T> values;
        values.resize(mTotal, std::numeric_limits<T>::max() / 2);
        EquivalentCompressWriter<T> writer;
        writer.Init(64);
        size_t compressLength = writer.CalculateCompressLength(values, 64);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(values);
        writer.DumpBuffer(buffer, compressLength);
        return { buffer, compressLength };
    }

    template<typename T>
    std::pair<uint8_t*, size_t> MakeBufferByRule()
    {
        std::vector<T> values;
        values.resize(mTotal, std::numeric_limits<T>::max() / 2);

        for (size_t i = 0; i < mTotal; i++)
        {
            if (i % 2 == 0) { values[i] = mSeed; }
        }
        EquivalentCompressWriter<T> writer;
        writer.Init(64);
        size_t compressLength = writer.CalculateCompressLength(values, 64);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(values);
        writer.DumpBuffer(buffer, compressLength);
        return { buffer, compressLength };
    }
    

    template<typename T>
    void DoUpdate(size_t pos, EquivalentCompressReader<T>& reader)
    {
        if (mNeedUpdate)
        {
            auto updatedValue = std::numeric_limits<T>::max() / 4 * 3;
            ASSERT_TRUE(reader.Update(mPos, updatedValue));
        }
    }

    template<typename T>
    void DoCheck(size_t pos, EquivalentCompressReader<T>& reader)
    {
        auto value = reader[pos];
        if (mOddEvenCheck)
        {
            if (pos % 2 == 0)
            {
                if (value != mSeed) { std::abort(); }
            }
            else
            {
                if (value != std::numeric_limits<T>::max() / 2) { std::abort(); }
            }
        }
        else
        {
            if (value != std::numeric_limits<T>::max() / 2 &&
                value != std::numeric_limits<T>::max() / 4 * 3)
            {
                std::cerr << "actual: " << value << " , " <<
                    " max: " << std::numeric_limits<T>::max() << std::endl;
                std::abort();
            }            
        }
    }

    void DoWrite() override;
    void DoRead(int* status) override;

private:
    std::string mLegacyDataPath;

    // for multithread test
    std::mt19937 mRandom;
    const size_t mTotal;
    volatile size_t mPos;
    uint8_t* mU64Buffer;
    uint8_t* mU32Buffer;
    EquivalentCompressReader<uint64_t> mU64Reader;
    EquivalentCompressReader<uint32_t> mU32Reader;
    util::BlockMemoryQuotaControllerPtr mMemoryController;
    bool mNeedUpdate;
    bool mSeed;
    bool mOddEvenCheck;    
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestReadLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestReadWithSessionReader);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestInplaceUpdate);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestMultiThreadReadWithFileReader);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateFromEqualSlot);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateForSmallerThanBaseValue);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateForExpandStoreSize);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateForExpandToMaxStoreType);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateForExpandToZeroBaseValue);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateFailWithoutExpandSliceArray);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestInplaceUpdatePerfLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdatePerf);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestRandomUpdateLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestReadLegacyData);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressReaderTest, TestExpandUpdateMultiThread);
IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENTCOMPRESSREADERTEST_H

