#include <iostream>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);

class ByteSliceReaderTest  : public INDEXLIB_TESTBASE
{
private:
    uint64_t mBlockSize;
    std::string mRootDir;
    std::string mFileName;
    util::BlockCachePtr mBlockCache;
    file_system::BlockFileNodePtr mBlockFileNode;
    
public:
    DECLARE_CLASS_NAME(ByteSliceReaderTest);
    void CaseSetUp() override
    {
        mBlockSize = 4;
        mRootDir = GET_TEST_DATA_PATH();
        mFileName = mRootDir + "block_file";
        mBlockCache.reset(new util::BlockCache);
    }

    void CaseTearDown() override
    {
        if (mBlockFileNode)
        {
            mBlockFileNode->Close();
        }
        mBlockCache.reset();
    }

    file_system::BlockFileNodePtr LoadFileNode(
            util::BlockCache* blockCache, const std::string& fileName)
    {
        file_system::BlockFileNodePtr blockFileNode(
                new file_system::BlockFileNode(blockCache, false));
        blockFileNode->Open(fileName, file_system::FSOT_CACHE);
        return blockFileNode;        
    }
    file_system::BlockFileNodePtr GenerateFileNode(
            int sliceLen, int sliceNumber,
            uint32_t val, uint32_t delta)
    {
        if (storage::FileSystemWrapper::IsExist(mFileName))
        {
            storage::FileSystemWrapper::DeleteFile(mFileName);
        }
        file_system::BufferedFileWriterPtr file(new file_system::BufferedFileWriter());
        file->Open(mFileName);
        std::vector<char> padding(sliceLen * sliceNumber);
        char* buffer = padding.data() + sliceLen - 1;
        VByteCompressor::WriteVUInt32(val, buffer);
        buffer = padding.data() + sliceLen * 2 - 1;
        VByteCompressor::WriteVUInt32(val + delta, buffer);
        buffer = padding.data() + sliceLen * 3 - 4;
        VByteCompressor::WriteVUInt32(val + delta * 2, buffer);
        file->Write((void*)padding.data(), padding.size());
        file->Close();
        mBlockCache->Init(100 * sliceLen, sliceLen, 1);
        return LoadFileNode(mBlockCache.get(), mFileName);
    }
    
    file_system::BlockFileNodePtr GenerateFileNode(
            int paddingLen, int sliceLen, int sliceNumber,
            uint8_t baseVal, bool incVal)
    {
        if (storage::FileSystemWrapper::IsExist(mFileName))
        {
            storage::FileSystemWrapper::DeleteFile(mFileName);
        }
        file_system::BufferedFileWriterPtr file(new file_system::BufferedFileWriter());
        file->Open(mFileName);
        std::vector<uint8_t> padding(paddingLen, baseVal - 1);
        std::vector<uint8_t> buf(sliceLen, baseVal);
        int curVal = baseVal;

        file->Write((void*)padding.data(), paddingLen);
        for (int i = 0; i < sliceNumber; ++i)
        {
            if (incVal)
            {
                for (auto& v : buf)
                {
                    v = curVal++;
                }
            }
            file->Write((void*)buf.data(), sliceLen);
        }
        file->Write((void*)padding.data(), paddingLen);        
        file->Close();
        mBlockCache->Init(100 * sliceLen, sliceLen, 1);
        return LoadFileNode(mBlockCache.get(), mFileName);
    }

    ByteSliceListPtr MakeBlockByteSliceList(
            int readOffset, int sliceLen, int sliceNumber, uint8_t defVal)
    {
        mBlockFileNode = GenerateFileNode(readOffset, sliceLen, sliceNumber, defVal, false);
        return ByteSliceListPtr(mBlockFileNode->Read(sliceLen * sliceNumber, readOffset));
    }
    
    ByteSliceListPtr MakeByteSliceList(int sliceLen, int sliceNumber, uint8_t defVal)
    {
        ByteSliceListPtr byteSliceList(new ByteSliceList());
        int i;
        for (i = 0; i < sliceNumber; ++i)
        {
            ByteSlice* byteSlice = ByteSlice::CreateObject(sliceLen);
            byteSliceList->Add(byteSlice);
            memset(byteSlice->data, defVal, sliceLen);
        }
        return byteSliceList;
    }

    ByteSliceListPtr MakeBlockByteSliceListIncValue(
            int paddingLen, int sliceLen, int sliceNumber, uint8_t baseVal)
    {
        mBlockFileNode = GenerateFileNode(
                paddingLen, sliceLen, sliceNumber, baseVal, true);
        return ByteSliceListPtr(mBlockFileNode->Read(sliceLen * sliceNumber, paddingLen));    
    }
    
    ByteSliceListPtr MakeByteSliceListIncValue(int sliceLen, int sliceNumber, uint8_t baseVal)
    {
        ByteSliceListPtr byteSliceList(new ByteSliceList());
        int i;
        uint8_t value = baseVal;
        for (i = 0; i < sliceNumber; ++i)
        {
            ByteSlice* byteSlice = ByteSlice::CreateObject(sliceLen);
            byteSliceList->Add(byteSlice);
            for (int j = 0; j < sliceLen; ++j)
            {
                byteSlice->data[j] = value++;
            }
        }
        return byteSliceList;
    }

    void TestCaseForReadByte()
    {
        int sliceLen = 100;
        int sliceNumber = 3;
        uint8_t defVal = 'z';

        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {
            ByteSliceReader reader(byteSliceList.get());
            int i;
            for (i = 0; i < sliceLen * sliceNumber; ++i)
            {
                uint8_t val = reader.ReadByte();
                EXPECT_EQ(defVal, val);
            }
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal));
        InnerCheck(MakeBlockByteSliceList(sliceLen/2, sliceLen, sliceNumber, defVal));
    }
    
    void TestCaseForReadInt16()
    {
        int sliceLen = 25 * sizeof(int16_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x84;

        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {
            ByteSliceReader reader(byteSliceList.get());
            size_t i;
            for (i = 0; i < sliceLen * sliceNumber / sizeof(int16_t); ++i)
            {
                int16_t val = reader.ReadInt16();
                INDEXLIB_TEST_EQUAL((int16_t)0x8484, val);
            }
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal)); 
        InnerCheck(MakeBlockByteSliceList(0, sliceLen, sliceNumber, defVal)); 
    }

    void TestCaseForReadInt32()
    {
        int sliceLen = 25 * sizeof(int32_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x84;
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {        
            ByteSliceReader reader(byteSliceList.get());
            size_t i;
            for (i = 0; i < sliceLen * sliceNumber / sizeof(int32_t); ++i)
            {
                int32_t val = reader.ReadInt32();
                INDEXLIB_TEST_TRUE(val == (int32_t)0x84848484);
            }
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal));
        InnerCheck(MakeBlockByteSliceList(sliceLen, sliceLen, sliceNumber, defVal)); 
    }

    void TestCaseForReadUInt32()
    {
        int sliceLen = 25 * sizeof(uint32_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x54;
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {                
            ByteSliceReader reader(byteSliceList.get());
            size_t i;
            for (i = 0; i < sliceLen * sliceNumber / sizeof(uint32_t); ++i)
            {
                uint32_t val = reader.ReadUInt32();
                INDEXLIB_TEST_TRUE(val == 0x54545454);
            }
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal)); 
        InnerCheck(MakeBlockByteSliceList(sliceLen + 1, sliceLen, sliceNumber, defVal));
    }

    void TestCaseForReadInt64()
    {
        int sliceLen = 25 * sizeof(int64_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x84;
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {                
            ByteSliceReader reader(byteSliceList.get());
            size_t i;
            for (i = 0; i < sliceLen * sliceNumber / sizeof(int64_t); ++i)
            {
                int64_t val = reader.ReadInt64();
                INDEXLIB_TEST_TRUE(val == (int64_t)0x8484848484848484LL);                
            }
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal)); 
        InnerCheck(MakeBlockByteSliceList(2 * sliceLen, sliceLen, sliceNumber, defVal)); 
    }
    
    void TestCaseForReadUInt64()
    {
        int sliceLen = 25 * sizeof(uint64_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x54;
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {                
            ByteSliceReader reader(byteSliceList.get());
            size_t i;
            for (i = 0; i < sliceLen * sliceNumber / sizeof(uint64_t); ++i)
            {
                uint64_t val = reader.ReadUInt64();
                INDEXLIB_TEST_TRUE(val == (uint64_t)0x5454545454545454LL);
            }
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal));
        InnerCheck(MakeBlockByteSliceList(sliceLen/2 + 3, sliceLen, sliceNumber, defVal)); 
    }

    void TestCaseForRead()
    {
        int sliceLen = 100 * sizeof(uint8_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x54;
        ByteSliceListPtr byteSliceList = MakeByteSliceList(sliceLen, sliceNumber, defVal);
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {
            ByteSliceReader reader(byteSliceList.get());
            const size_t unitSize = 40;
            size_t readCount = sliceLen * sliceNumber / unitSize;
            uint8_t buffer[unitSize];
            uint8_t refBuffer[unitSize];
            memset(refBuffer, defVal, unitSize);

            for (size_t i = 0; i < readCount; ++i)
            {
                uint32_t ret = reader.Read(buffer, unitSize);
                INDEXLIB_TEST_TRUE(ret == unitSize);
                INDEXLIB_TEST_TRUE(memcmp(buffer, refBuffer, unitSize) == 0);
                memset(buffer, 0, unitSize);
            }
            uint32_t ret = reader.Read(buffer, unitSize);
            size_t lastReadSize = sliceLen * sliceNumber - readCount * unitSize;
            INDEXLIB_TEST_EQUAL((uint32_t)lastReadSize, ret);
            INDEXLIB_TEST_TRUE(memcmp(buffer, refBuffer, lastReadSize) == 0);

        
            memset(buffer, defVal, unitSize);
            ret = reader.Read(buffer, unitSize);
            INDEXLIB_TEST_TRUE(ret == 0);
            INDEXLIB_TEST_TRUE(memcmp(buffer, refBuffer, unitSize) == 0);
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal));
        InnerCheck(MakeBlockByteSliceList(sliceLen/2 , sliceLen, sliceNumber, defVal));
    }

    void TestCaseForReadMayCopy()
    {
        int sliceLen = 100 * sizeof(uint8_t);
        int sliceNumber = 3;
        uint8_t defVal = 0x54;
        ByteSliceListPtr byteSliceList = MakeByteSliceList(sliceLen, sliceNumber, defVal);
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {
            ByteSliceReader reader(byteSliceList.get());
            const int unitSize = 40;
            int readCount = sliceLen * sliceNumber / unitSize;
            uint8_t buffer[unitSize];
            uint8_t refBuffer[unitSize];
            void *tmpBuffer = buffer;
            memset(refBuffer, defVal, unitSize);

            for (int i = 0; i < readCount; ++i)
            {
                tmpBuffer = buffer;
                int ret = reader.ReadMayCopy(tmpBuffer, unitSize);
                INDEXLIB_TEST_TRUE(ret == unitSize);
                INDEXLIB_TEST_TRUE(memcmp(tmpBuffer, refBuffer, unitSize) == 0);
                memset(buffer, 0, unitSize);
            }
            tmpBuffer = buffer;
            int ret = reader.ReadMayCopy(tmpBuffer, unitSize);
            int lastReadSize = sliceLen * sliceNumber - readCount * unitSize;
            INDEXLIB_TEST_TRUE(ret == lastReadSize);
            INDEXLIB_TEST_TRUE(memcmp(tmpBuffer, refBuffer, lastReadSize) == 0);

            memset(buffer, defVal, unitSize);
            tmpBuffer = buffer;
            ret = reader.ReadMayCopy(tmpBuffer, unitSize);
            INDEXLIB_TEST_TRUE(ret == 0);
            INDEXLIB_TEST_TRUE(memcmp(tmpBuffer, refBuffer, unitSize) == 0);
        };
        InnerCheck(MakeByteSliceList(sliceLen, sliceNumber, defVal));
        InnerCheck(MakeBlockByteSliceList(0, sliceLen, sliceNumber, defVal)); 
    }

    void TestCaseForSeek()
    {
        const int sliceLen = 100 * sizeof(uint8_t);
        const int sliceNumber = 3;
        uint8_t baseVal = 0x54;
        ByteSliceListPtr byteSliceList = MakeByteSliceListIncValue(sliceLen, sliceNumber, baseVal);
        auto InnerCheck = [&] (const ByteSliceListPtr& byteSliceList)
        {
            ByteSliceReader reader(byteSliceList.get());
            const int count = 6;
            uint32_t offsets[count] = {0, 50, 102, 140, 266, 299};
            int ansReturned[count]  = {0, 50, 102, 140, 266, 299};
            uint32_t ansOffsets[count]   = {0, 50, 102, 140, 266, 299};
        
            for (int i = 0; i < count; ++i)
            {
                int ret;
                ret = reader.Seek(offsets[i]);
                INDEXLIB_TEST_TRUE(ret == ansReturned[i]);
                INDEXLIB_TEST_TRUE(reader.Tell() == ansOffsets[i]);
            }
        };
        InnerCheck(MakeByteSliceListIncValue(sliceLen, sliceNumber, baseVal));
        InnerCheck(MakeBlockByteSliceListIncValue(sliceLen/2, sliceLen, sliceNumber, baseVal)); 
    }

    void TestCaseReadVInt32WithBlock()
    {
        mBlockFileNode = GenerateFileNode(
                100, 3, 999, 1000);
        ByteSliceListPtr sliceList(mBlockFileNode->Read(250, 50));
        {
            ByteSliceReader reader(sliceList.get());
            reader.Seek(246);
            uint32_t readValue = reader.ReadVInt32();
            ASSERT_EQ((uint32_t)2999, readValue);
        }        
        {
            ByteSliceReader reader(sliceList.get());
            reader.Seek(149);
            uint32_t readValue = reader.ReadVInt32();
            ASSERT_EQ((uint32_t)1999, readValue);
        }
        {
            ByteSliceReader reader(sliceList.get());
            reader.Seek(49);
            uint32_t readValue = reader.ReadVInt32();
            ASSERT_EQ((uint32_t)999, readValue);
            reader.Seek(246);
            readValue = reader.ReadVInt32();
            ASSERT_EQ((uint32_t)2999, readValue);
        }
    }
    void TestCaseForReadVInt32()
    {
        ByteSliceListPtr byteSliceList(new ByteSliceList());
        ByteSlice *byteSlice = ByteSlice::CreateObject(3);
        
        byteSliceList->Add(byteSlice);
        ByteSliceReader reader(byteSliceList.get());

        uint32_t value = 2;
        uint8_t* cursor = byteSlice->data;
        *cursor++ = value;
        
        uint32_t readValue = reader.ReadVInt32();
        INDEXLIB_TEST_EQUAL(value, readValue);

        value = 200;
        *cursor++ = 0x80 | (value & 0x7F);
        *cursor = (value >> 7);

        readValue = reader.ReadVInt32();
        INDEXLIB_TEST_EQUAL(value, readValue);

        byteSliceList->Clear(NULL);
    }

    void DumpByteSliceList(ByteSliceWriter& byteSliceWriter, const std::string& fileName)
    {
        file_system::BufferedFileWriterPtr fileWriter(
                new file_system::BufferedFileWriter());
        fileWriter->Open(fileName);
        byteSliceWriter.Dump(fileWriter);
        fileWriter->Close();
    }

    void TestCaseForReadInt32NotAligned()
    {
        const uint32_t ITEM_NUM = 3;

        ByteSliceWriter writer;
        uint32_t i;

        writer.WriteByte(1);

        for (i = 0; i < ITEM_NUM; i++)
        {
            writer.WriteInt32((i << 8) + 3);
        }
        DumpByteSliceList(writer, mFileName);
        uint32_t value;
        auto InnerCheck = [&] (ByteSliceList* byteSliceList)
        {
            ByteSliceReader reader(byteSliceList);
            uint8_t b = reader.ReadByte();
            INDEXLIB_TEST_EQUAL(1, b);

            for (i = 0; i < ITEM_NUM; i++)
            {
                value = reader.ReadInt32();
                INDEXLIB_TEST_EQUAL(value, (i << 8) + 3);
            }
        };
        InnerCheck(writer.GetByteSliceList());
        mBlockCache->Init(100 * 4, 4, 1);
        auto blockFileNode = LoadFileNode(mBlockCache.get(), mFileName);
        ByteSliceList* blockByteSliceList = blockFileNode->Read(ITEM_NUM * sizeof(uint32_t) + 1, 0);
        InnerCheck(blockByteSliceList);
        delete blockByteSliceList;
    }
    
    void TestCaseForReadVInt32WithWriter()
    {
        ByteSliceWriter writer;
        uint32_t i;
        const uint32_t ITEM_NUM = 1000;

        for (i = 0; i < ITEM_NUM; i++)
        {
            writer.WriteVInt(i * 99 + 3);
        }
        size_t len = writer.GetSize();
        DumpByteSliceList(writer, mFileName);
        uint32_t value;
        ByteSliceReader reader(writer.GetByteSliceList());
        for (i = 0; i < ITEM_NUM; i++)
        {
            value = reader.ReadVInt32();
            INDEXLIB_TEST_EQUAL(value, i * 99 + 3);
        }

        mBlockCache->Init(2000 * 4, 4, 1);
        auto blockFileNode = LoadFileNode(mBlockCache.get(), mFileName);
        auto blockByteSliceList = blockFileNode->Read(len, 0);
        ByteSliceReader reader1(blockByteSliceList);
        for (i = 0; i < ITEM_NUM; i++)
        {
            value = reader1.ReadVInt32();
            INDEXLIB_TEST_EQUAL(value, i * 99 + 3);
        }
        delete blockByteSliceList;
    }

    void TestCaseForCopyStructor()
    {
        ByteSliceWriter writer;
        uint32_t i;
        const uint32_t ITEM_NUM = 1000;

        for (i = 0; i < ITEM_NUM; i++)
        {
            writer.WriteInt32(i * 99 + 3);
        }

        uint32_t value;
        ByteSliceReader reader(writer.GetByteSliceList());
        for (i = 0; i < ITEM_NUM/2; i++)
        {
            value = reader.ReadInt32();
            INDEXLIB_TEST_EQUAL(value, i * 99 + 3);
        }

        ByteSliceReader reader2(reader);
        for (; i < ITEM_NUM; i++)
        {
            value = reader2.ReadInt32();
            INDEXLIB_TEST_EQUAL(value, i * 99 + 3);
        }
    }

    void TestCaseForPeekInt32()
    {
        const uint32_t ITEM_NUM = 120;

        ByteSliceWriter writer;
        uint32_t i;

        writer.WriteByte(1);

        for (i = 0; i < ITEM_NUM; i++)
        {
            writer.WriteInt32((i << 8) + 3);
        }

        size_t listLen = writer.GetSize();
        DumpByteSliceList(writer, mFileName);

        auto InnerCheck = [&] (ByteSliceList* byteSliceList)
        {
            ByteSliceReader reader(byteSliceList);
            uint8_t b = reader.ReadByte();
            INDEXLIB_TEST_EQUAL(1, b);

            for (i = 0; i < ITEM_NUM; i++)
            {
                int32_t value1 = reader.PeekInt32();
                int32_t value2 = reader.ReadInt32();
                EXPECT_EQ((int32_t)(i << 8) + 3, value1);
                EXPECT_EQ((int32_t)(i << 8) + 3, value2);
            }
        };
        InnerCheck(writer.GetByteSliceList());
        mBlockCache->Init(ITEM_NUM * 2 * 4, 4, 1);
        auto blockFileNode = LoadFileNode(mBlockCache.get(), mFileName);
        ByteSliceList* blockByteSliceList = blockFileNode->Read(listLen, 0);
        InnerCheck(blockByteSliceList);
        delete blockByteSliceList;
    }

    void TestCaseForSeekAndPeekInt32()
    {
        const uint32_t ITEM_NUM = 120;

        ByteSliceWriter writer;
        uint32_t i;

        for (i = 0; i < ITEM_NUM; i++)
        {
            writer.WriteInt32((i << 8) + 3);
        }

        auto InnerCheck = [&] (ByteSliceList* byteSliceList)
        {
            ByteSliceReader reader(byteSliceList);
            reader.Seek(10 * sizeof(int32_t));
            int32_t value1 = reader.PeekInt32();
            int32_t value2 = reader.ReadInt32();
            INDEXLIB_TEST_EQUAL(value1, (10 << 8) + 3);
            INDEXLIB_TEST_EQUAL(value2, (10 << 8) + 3);
        };
        InnerCheck(writer.GetByteSliceList());
        DumpByteSliceList(writer, mFileName);
        size_t listLen = writer.GetSize();
        
        mBlockCache->Init(ITEM_NUM * 2 * 4, 4, 1);
        auto blockFileNode = LoadFileNode(mBlockCache.get(), mFileName);
        ByteSliceList* blockByteSliceList = blockFileNode->Read(listLen, 0);
        InnerCheck(blockByteSliceList);
        delete blockByteSliceList;
    }

    void TestCaseForPeekInt32AndReadMayCopy()
    {
        const uint32_t ITEM_NUM = 1200;

        ByteSliceWriter writer;
        uint32_t i;

        for (i = 0; i < ITEM_NUM; i++)
        {
            writer.WriteInt32((i << 8) + 3);
        }

        auto InnerCheck = [&] (ByteSliceList* byteSliceList)
        {
            ByteSliceReader reader(byteSliceList);
            int32_t value1 = reader.PeekInt32();
            INDEXLIB_TEST_EQUAL(value1, (0 << 8) + 3);

            int32_t buffer[128];
            void* bufPtr = (void*)buffer;
            reader.ReadMayCopy(bufPtr, 128 * sizeof(int32_t));
            i = 0;
            for (size_t j = 0; j < 128; ++i, ++j)
            {
                INDEXLIB_TEST_EQUAL((i << 8) + 3, (uint32_t)buffer[j]);
            }

            int32_t value2 = reader.PeekInt32();
            INDEXLIB_TEST_EQUAL((i << 8) + 3, (uint32_t)value2);

            bufPtr = (void*)buffer;
            reader.ReadMayCopy(bufPtr, 113 * sizeof(int32_t));
            for (size_t j = 0; j < 113; ++i, ++j)
            {
                INDEXLIB_TEST_EQUAL((i << 8) + 3, (uint32_t)buffer[j]);
            }
        };
        InnerCheck(writer.GetByteSliceList());
        size_t listLen = writer.GetSize();
        DumpByteSliceList(writer, mFileName);
        mBlockCache->Init(ITEM_NUM * 2 * 4, 4, 1);        
        auto blockFileNode = LoadFileNode(mBlockCache.get(), mFileName);
        ByteSliceList* blockByteSliceList = blockFileNode->Read(listLen, 0);
        InnerCheck(blockByteSliceList);
        delete blockByteSliceList;        
    }


};

INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadByte);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadInt16);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadUInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadInt64);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadUInt64);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForSeek);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadVInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseReadVInt32WithBlock);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadInt32NotAligned);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadVInt32WithWriter);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForCopyStructor);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForReadMayCopy);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForPeekInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForSeekAndPeekInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceReaderTest, TestCaseForPeekInt32AndReadMayCopy);

IE_NAMESPACE_END(common);
