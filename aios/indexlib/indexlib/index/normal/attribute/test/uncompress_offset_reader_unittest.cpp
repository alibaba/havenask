#include "indexlib/index/normal/attribute/test/uncompress_offset_reader_unittest.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UncompressOffsetReaderTest);

UncompressOffsetReaderTest::UncompressOffsetReaderTest()
{
}

UncompressOffsetReaderTest::~UncompressOffsetReaderTest()
{
}

void UncompressOffsetReaderTest::CaseSetUp()
{
}

void UncompressOffsetReaderTest::CaseTearDown()
{
}

void UncompressOffsetReaderTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string offsetStr = "0,3,5,7,9,10,12";
    vector<uint64_t> offsetVec;
    StringUtil::fromString(offsetStr, offsetVec, ",");

    uint8_t* buffer = (uint8_t*)offsetVec.data();
    uint64_t bufferLen = offsetVec.size() * sizeof(uint64_t);

    uint32_t docCount = offsetVec.size() - 1;
    UncompressOffsetReader offsetReader;
    ASSERT_NO_THROW(offsetReader.Init(buffer, bufferLen, docCount));

    ASSERT_TRUE(!offsetReader.IsU32Offset());

    for (size_t i = 0; i < offsetVec.size(); i++)
    {
        ASSERT_EQ(offsetVec[i], offsetReader.GetOffset((docid_t)i));
    }
}

void UncompressOffsetReaderTest::TestInit()
{
    vector<uint64_t> offsetVec;
    offsetVec.resize(10);

    uint8_t* buffer = (uint8_t*)offsetVec.data();
    uint32_t docCount = 9;

    {
        // u32
        UncompressOffsetReader offsetReader;
        ASSERT_NO_THROW(offsetReader.Init(buffer, 80, docCount));
        ASSERT_FALSE(offsetReader.IsU32Offset());
    }

    {
        // u64
        UncompressOffsetReader offsetReader;
        ASSERT_NO_THROW(offsetReader.Init(buffer, 40, docCount));
        ASSERT_TRUE(offsetReader.IsU32Offset());
    }

    {
        // invalid
        UncompressOffsetReader offsetReader;
        ASSERT_ANY_THROW(offsetReader.Init(buffer, 20, docCount));
    }
}

void UncompressOffsetReaderTest::TestExtendOffsetVector()
{
    // pipeLineNum is 8
    InnerTestExtendOffsetVector(1);
    InnerTestExtendOffsetVector(5);
    InnerTestExtendOffsetVector(8);
    InnerTestExtendOffsetVector(10);
    InnerTestExtendOffsetVector(1000000); 
}

void UncompressOffsetReaderTest::InnerTestExtendOffsetVector(uint32_t docCount)
{
    TearDown();
    SetUp();

    vector<uint32_t> expectOffsetVec;
    for (uint32_t i = 0; i < docCount; ++i)
    {
        expectOffsetVec.push_back(i);
    }

    DirectoryPtr segmentDirectory = GET_SEGMENT_DIRECTORY();
    SliceFilePtr sliceFile = segmentDirectory->CreateSliceFile(
            "expand_slice", docCount * sizeof(uint64_t), 1);
    SliceFileReaderPtr sliceFileReader = sliceFile->CreateSliceFileReader();

    UncompressOffsetReader offsetReader;
    offsetReader.Init((uint8_t*)expectOffsetVec.data(), 
                      expectOffsetVec.size() * sizeof(uint32_t),
                      expectOffsetVec.size() - 1,
                      sliceFileReader);

    offsetReader.ExtendU32OffsetToU64Offset(sliceFileReader);
    ASSERT_TRUE(offsetReader.mU64Buffer != NULL);
    ASSERT_FALSE(offsetReader.IsU32Offset());
    for (size_t docId = 0; docId < expectOffsetVec.size(); ++docId)
    {
        uint64_t offset = offsetReader.GetOffset(docId);
        ASSERT_EQ((uint64_t)expectOffsetVec[docId], offset);
    }
}

IE_NAMESPACE_END(index);

