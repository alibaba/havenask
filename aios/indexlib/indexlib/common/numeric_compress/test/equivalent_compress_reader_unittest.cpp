#include "indexlib/common/numeric_compress/test/equivalent_compress_reader_unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, EquivalentCompressReaderTest);

EquivalentCompressReaderTest::EquivalentCompressReaderTest()
    : mRandom(0)
    , mTotal(10000)
    , mPos(0)
    , mU64Buffer(NULL)
    , mU32Buffer(NULL)
    , mNeedUpdate(true)
    , mSeed(0u)
    , mOddEvenCheck(false)
{
}

EquivalentCompressReaderTest::~EquivalentCompressReaderTest()
{
}

void EquivalentCompressReaderTest::CaseSetUp()
{
    mLegacyDataPath = FileSystemWrapper::JoinPath(
            TEST_DATA_PATH, "legacy_equal_compress_data");
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();    
}

void EquivalentCompressReaderTest::CaseTearDown()
{
    if (mU64Buffer)
    {
        delete[] mU64Buffer;
        mU64Buffer = NULL;
    }
    if (mU32Buffer)
    {
        delete[] mU32Buffer;
        mU32Buffer = NULL;
    }
}

void EquivalentCompressReaderTest::TestInit()
{
    {
        // slotItemNum is 128, totalItemCount 0
        uint32_t buffer[] = {0, 7};
        EquivalentCompressReaderBase array((const uint8_t*)buffer);
        
        ASSERT_EQ((uint32_t)0, array.mItemCount);
        ASSERT_EQ((uint32_t)7, array.mSlotBitNum);
        ASSERT_EQ((uint32_t)127, array.mSlotMask);
        ASSERT_EQ((uint32_t)0, array.mSlotNum);
        ASSERT_TRUE(array.mSlotBaseAddr == NULL);
        ASSERT_TRUE(array.mValueBaseAddr == NULL);
    }
    {
        // slotItemNum is 128, totalItemCount 3
        uint32_t buffer[] = {3, 7};
        EquivalentCompressReader<uint32_t> array((const uint8_t*)buffer);
        
        ASSERT_EQ((uint32_t)3, array.mItemCount);
        ASSERT_EQ((uint32_t)7, array.mSlotBitNum);
        ASSERT_EQ((uint32_t)127, array.mSlotMask);
        ASSERT_EQ((uint32_t)1, array.mSlotNum);

        // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
        ASSERT_EQ((uint64_t*)((uint8_t*)buffer + sizeof(buffer)),
                    array.mSlotBaseAddr);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
        // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
        // + slotItemNum * slotItemSize : sizeof(uint64_t)
        ASSERT_EQ((uint8_t*)buffer + sizeof(buffer) + sizeof(uint64_t), 
                    array.mValueBaseAddr);
#pragma GCC diagnostic pop
    }
    {
        // slotItemNum is 128, totalItemCount 256
        uint32_t buffer[] = {256, 7};
        EquivalentCompressReader<uint64_t> array((const uint8_t*)buffer);
        
        ASSERT_EQ((uint32_t)256, array.mItemCount);
        ASSERT_EQ((uint32_t)7, array.mSlotBitNum);
        ASSERT_EQ((uint32_t)127, array.mSlotMask);
        ASSERT_EQ((uint32_t)2, array.mSlotNum);

        // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
        ASSERT_EQ((uint64_t*)((uint8_t*)buffer + sizeof(buffer)), 
                    array.mSlotBaseAddr);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
        // itemCount : sizeof(uint32_t) + slotNum : sizeof(uint32_t)
        // + slotItemNum * slotItemSize : sizeof(uint64_t)
        ASSERT_EQ((uint8_t*)buffer + sizeof(buffer) + 2 * sizeof(uint64_t), 
                    array.mValueBaseAddr);
    }
#pragma GCC diagnostic pop
}


// TODO: replace EquivalentCompressArray with EquivalentCompressWriter
void EquivalentCompressReaderTest::TestReadLongCaseTest()
{
    {
        uint32_t value[] = {17,  2,   3, 9,   17,   5,  23, 68,
                            281, 321, 3, 647, 823, 31, 478, 83,
                            3,   3,   3,   3};

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(8);
        size_t compressLength = writer.CalculateCompressLength(value, 20, 8);

        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<uint32_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            } 
        }
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            }
        }
        delete[] buffer;
    }

    {
        uint64_t value[] = {17,  2,   3, 9,   17,   5,  23, 68,
                            281, 321, 3, 647, ((uint64_t)1 << 63), 31, 478, 83,
                            3,   3,   3,   3};

        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(8);
        size_t compressLength = writer.CalculateCompressLength(value, 20, 8);

        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<uint64_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            }
        }
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            } 
        }
        delete[] buffer;
    }

    {
        int32_t value[] = {-17,  2,   3, 9,   17,   5,  23, 68,
                           281, 321, 3, 647, -823, 31, 478, 83,
                           3,   3,   3,   3};

        EquivalentCompressWriter<int32_t> writer;
        writer.Init(8);
        size_t compressLength = writer.CalculateCompressLength(value, 20, 8);

        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<int32_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            }
        }
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<int32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            }             
        }
        delete[] buffer;
    }

    {
        int64_t value[] = {17,  2,   3, 9,   17,   5,  23, 68,
                            281, 321, 3, -647, -((int64_t)1 << 60), 31, 478, 83,
                            3,   3,   3,   3};

        EquivalentCompressWriter<int64_t> writer;
        writer.Init(8);
        size_t compressLength = writer.CalculateCompressLength(value, 20, 8);

        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<int64_t> compArray(buffer);
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            }
        }
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<int64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            } 
        }
        delete[] buffer;
    }

    // mass random data
    {
        vector<uint32_t> array;
        MakeData<uint32_t>(20000000, array, 97);

        EquivalentCompressWriter<uint32_t> writer;
        writer.Init(1024);
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<uint32_t> compArray(buffer);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], compArray[i]);
            }            
        }
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(array[i], compArray.Get(i));
            } 
        }
        delete[] buffer;
    }
    {
        vector<uint64_t> array;
        MakeData<uint64_t>(20000000, array, 97);
        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(1024);
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        int64_t latency = 0;
        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<uint64_t> compArray(buffer);
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], compArray[i]);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        latency = 0;
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], compArray.Get(i));
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;        
        delete[] buffer;
    }
    {
        vector<int32_t> array;
        MakeData<int32_t>(20000000, array, 97);

        EquivalentCompressWriter<int32_t> writer;
        writer.Init(1024);
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);
        EquivalentCompressReader<int32_t> compArray(buffer);
        for (size_t i = 0; i < array.size(); i++)
        {
            ASSERT_EQ(array[i], compArray[i]);
        }
        delete[] buffer;
    }

    {
        vector<int64_t> array;
        MakeData<int64_t>(20000000, array, 97);
        
        EquivalentCompressWriter<int64_t> writer;
        writer.Init(1024);
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        ASSERT_EQ(compSize, compressLength);
        EquivalentCompressReader<int64_t> compArray(buffer);
        for (size_t i = 0; i < array.size(); i++)
        {
            ASSERT_EQ(array[i], compArray[i]);
        }
        delete[] buffer;
    }
}

void EquivalentCompressReaderTest::TestReadWithSessionReader()
{
    {
        vector<uint64_t> array;
        MakeData<uint64_t>(20000000, array, 97);
        EquivalentCompressWriter<uint64_t> writer;
        writer.Init(1024);
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        int64_t latency = 0;
        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<uint64_t> compArray(buffer);
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], compArray[i]);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        latency = 0;
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<uint64_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            EquivalentCompressSessionReader<uint64_t> sessionReader = compArray.CreateSessionReader();
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], sessionReader.Get(i));
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;        
        delete[] buffer; 
    }
    {
        vector<int32_t> array;
        MakeData<int32_t>(20000000, array, 97);
        EquivalentCompressWriter<int32_t> writer;
        writer.Init(1024);
        size_t compressLength = writer.CalculateCompressLength(array, 1024);
        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(array);
        size_t compSize = writer.DumpBuffer(buffer, compressLength);

        int64_t latency = 0;
        ASSERT_EQ(compSize, compressLength);
        {
            EquivalentCompressReader<int32_t> compArray(buffer);
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], compArray[i]);
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;
        latency = 0;
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<int32_t> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            EquivalentCompressSessionReader<int32_t> sessionReader = compArray.CreateSessionReader();
            ScopedTime timer(latency);
            for (size_t i = 0; i < array.size(); i++)
            {
                ASSERT_EQ(array[i], sessionReader.Get(i));
            }
        }
        cout << "time cost for access " << array.size() << " items: " << latency / 1000 << " ms" << endl;        
        delete[] buffer;            
    }
    {
        double value[] = {17,  2,   3, 9,   17,   5,  23, 68,
                          281, 321, 3, 647, 823, 31, 478, 83,
                          3,   3,   3,   3};

        EquivalentCompressWriter<double> writer;
        writer.Init(8);
        size_t compressLength = writer.CalculateCompressLength(value, 20, 8);

        uint8_t *buffer = new uint8_t[compressLength];
        writer.CompressData(value, 20);
        writer.DumpBuffer(buffer, compressLength);

        {
            EquivalentCompressReader<double> compArray(buffer);
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], compArray.Get(i));
            }            
        }
        {
            FileNodePtr fileNode(new InMemFileNode(
                                     (size_t)compressLength, false, LoadConfig(), mMemoryController));
            string path = "in_mem_file"; 
            fileNode->Open(path, FSOT_IN_MEM);
            fileNode->Write(buffer, compressLength);
            NormalFileReaderPtr fileReader(new NormalFileReader(fileNode));
            EquivalentCompressReader<double> compArray;
            ASSERT_TRUE(compArray.Init(fileReader));
            auto sessionReader = compArray.CreateSessionReader();
            for (size_t i = 0; i < 20; i++)
            {
                ASSERT_EQ(value[i], sessionReader.Get(i));
            } 
        }
        delete[] buffer;        
    }

}

void EquivalentCompressReaderTest::TestMultiThreadReadWithFileReader()
{
    mPos = 0;
    util::SimpleMemoryQuotaControllerPtr controller(
            new util::SimpleMemoryQuotaController(
                    util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));

    mSeed = 3;
    mOddEvenCheck = true;
    
    auto u64BufferInfo = MakeBufferByRule<uint64_t>();
    mU64Buffer = u64BufferInfo.first;

    auto u32BufferInfo = MakeBufferByRule<uint32_t>();
    mU32Buffer = u32BufferInfo.first;

    FileNodePtr fileNode1(new InMemFileNode(
                             (size_t)u64BufferInfo.second, false, LoadConfig(), mMemoryController));
    FileNodePtr fileNode2(new InMemFileNode(
                             (size_t)u32BufferInfo.second, false, LoadConfig(), mMemoryController));

    string path1 = "in_mem_file1"; 
    fileNode1->Open(path1, FSOT_IN_MEM);
    fileNode1->Write(mU64Buffer, u64BufferInfo.second);

    string path2 = "in_mem_file2"; 
    fileNode2->Open(path2, FSOT_IN_MEM);
    fileNode2->Write(mU32Buffer, u32BufferInfo.second);

    NormalFileReaderPtr fileReader1(new NormalFileReader(fileNode1));
    NormalFileReaderPtr fileReader2(new NormalFileReader(fileNode2));    
    
    ASSERT_TRUE(mU64Reader.Init(fileReader1));
    ASSERT_TRUE(mU32Reader.Init(fileReader2));
    mNeedUpdate = false;
    DoMultiThreadTest(5, 10);
}

void EquivalentCompressReaderTest::TestInplaceUpdate()
{
    InnerTestInplaceUpdate<uint32_t>(1, 0);         // 1bit update
    InnerTestInplaceUpdate<uint32_t>(2, "1,3");     // 2bit update
    InnerTestInplaceUpdate<uint32_t>(5, "1,3,10");  // 4bit update

    InnerTestInplaceUpdate<uint32_t>(20, "1,3,10,30");        // 1byte update
    InnerTestInplaceUpdate<uint32_t>(300, "1,3,10,30,500");   // 2byte update
    InnerTestInplaceUpdate<uint32_t>(70000, "1,3,10,30,500,77777"); // 4byte update

    InnerTestInplaceUpdate<uint64_t>(1, 0);         // 1bit update
    InnerTestInplaceUpdate<uint64_t>(2, "1,3");     // 2bit update
    InnerTestInplaceUpdate<uint64_t>(5, "1,3,10");  // 4bit update

    InnerTestInplaceUpdate<uint64_t>(20, "1,3,10,30");        // 1byte update
    InnerTestInplaceUpdate<uint64_t>(300, "1,3,10,30,500");   // 2byte update
    InnerTestInplaceUpdate<uint64_t>(70000, "1,3,10,30,500,77777"); // 4byte update

    InnerTestInplaceUpdate<uint64_t>(((uint64_t)1<<33), "1,3,10,30,500,77777"); // 8byte update
    InnerTestInplaceUpdate<uint64_t>(((uint64_t)1<<33), ((uint64_t)1<<43)); // 8byte update
}

void EquivalentCompressReaderTest::TestExpandUpdateFromEqualSlot()
{
    InnerTestExpandUpdateFromEqualSlot<uint32_t>();
    InnerTestExpandUpdateFromEqualSlot<uint64_t>();
}

void EquivalentCompressReaderTest::TestExpandUpdateForSmallerThanBaseValue()
{
    InnerTestExpandUpdateForSmallerThanBaseValue<uint32_t>();
    InnerTestExpandUpdateForSmallerThanBaseValue<uint64_t>();
}

void EquivalentCompressReaderTest::TestExpandUpdateForExpandStoreSize()
{
    InnerTestExpandUpdateForExpandStoreSize<uint32_t>(5);
    InnerTestExpandUpdateForExpandStoreSize<uint64_t>(6);
}

void EquivalentCompressReaderTest::TestExpandUpdateForExpandToMaxStoreType()
{
    {
        // T = uint32_t
        std::vector<size_t> updateIdxs;
        std::vector<uint32_t> updateValues;

        updateIdxs.push_back(2);
        uint32_t updateValue = (uint32_t)1000 + std::numeric_limits<uint16_t>::max();
        updateValues.push_back(updateValue);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte, store noUseBytes
        expandSize += sizeof(uint32_t) * 64; // delta
        expandSize += sizeof(uint32_t); // header

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

void EquivalentCompressReaderTest::TestExpandUpdateForExpandToZeroBaseValue()
{
    {
        // T = uint32_t
        std::vector<size_t> updateIdxs;
        std::vector<uint32_t> updateValues;

        updateIdxs.push_back(2);
        updateValues.push_back(100);

        size_t expandSize = sizeof(EquivalentCompressUpdateMetrics); // first 16 byte, store noUseBytes
        expandSize += sizeof(uint32_t) * 64; // delta
        expandSize += sizeof(uint32_t); // header

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

void EquivalentCompressReaderTest::TestExpandUpdateFailWithoutExpandSliceArray()
{
    std::vector<uint32_t> values;
    values.resize(10, 0);

    EquivalentCompressWriter<uint32_t> writer;
    writer.Init(64);

    size_t compressLength = writer.CalculateCompressLength(values, 64);
    uint8_t *buffer = new uint8_t[compressLength];
    writer.CompressData(values);
    writer.DumpBuffer(buffer, compressLength);

    EquivalentCompressReader<uint32_t> compArray;
    compArray.Init(buffer);
    ASSERT_FALSE(compArray.Update(0, 1));
    delete[] buffer;
}

void EquivalentCompressReaderTest::TestInplaceUpdatePerfLongCaseTest()
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

void EquivalentCompressReaderTest::TestExpandUpdatePerf()
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


void EquivalentCompressReaderTest::TestRandomUpdateLongCaseTest()
{
    InnerTestRandomUpdate<uint32_t>();
    InnerTestRandomUpdate<int32_t>();
    InnerTestRandomUpdate<uint64_t>();
    InnerTestRandomUpdate<int64_t>();
}

void EquivalentCompressReaderTest::TestReadLegacyData()
{
    InnerTestReadLegacyData<uint32_t>();
    InnerTestReadLegacyData<uint64_t>();
}

void EquivalentCompressReaderTest::TestExpandUpdateMultiThread()
{
    mPos = 0;
    util::SimpleMemoryQuotaControllerPtr controller(
            new util::SimpleMemoryQuotaController(
                    util::MemoryQuotaControllerCreator::CreateBlockMemoryController()));

    auto u64BufferInfo = MakeBuffer<uint64_t>();
    mU64Buffer = u64BufferInfo.first;

    auto u32BufferInfo = MakeBuffer<uint32_t>();
    mU32Buffer = u32BufferInfo.first;
    util::BytesAlignedSliceArrayPtr u64ExpandSliceArray(
            new util::BytesAlignedSliceArray(controller,
                    EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                    EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));
    util::BytesAlignedSliceArrayPtr u32ExpandSliceArray(
            new util::BytesAlignedSliceArray(controller,
                    EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                    EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));
    mU64Reader.Init(mU64Buffer, u64BufferInfo.second, u64ExpandSliceArray);
    mU32Reader.Init(mU32Buffer, u32BufferInfo.second, u32ExpandSliceArray);

    DoMultiThreadTest(5, 10);
}

void EquivalentCompressReaderTest::DoWrite()
{
    while (!IsFinished())
    {
        mPos = mRandom() % mTotal;
        DoUpdate(mPos, mU64Reader);
        DoUpdate(mPos, mU32Reader);
    }
}

void EquivalentCompressReaderTest::DoRead(int *status)
{
    while (!IsFinished())
    {
        auto pos = mPos;
        DoCheck(pos, mU64Reader);
        DoCheck(pos, mU32Reader);
    }
}

IE_NAMESPACE_END(common);

