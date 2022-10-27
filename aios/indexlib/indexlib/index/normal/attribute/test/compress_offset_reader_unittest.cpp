#include "indexlib/index/normal/attribute/test/compress_offset_reader_unittest.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CompressOffsetReaderTest);

CompressOffsetReaderTest::CompressOffsetReaderTest()
{
}

CompressOffsetReaderTest::~CompressOffsetReaderTest()
{
}

void CompressOffsetReaderTest::CaseSetUp()
{
}

void CompressOffsetReaderTest::CaseTearDown()
{
}

void CompressOffsetReaderTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");

    string offsetStr = "0,3,5,7,9,10,12";
    vector<uint64_t> offsetVec;
    StringUtil::fromString(offsetStr, offsetVec, ",");
    uint8_t* buffer = NULL;
    size_t bufferLen;
    uint32_t docCount = offsetVec.size() - 1;
    PrepareCompressedOffsetBuffer<uint64_t>(offsetVec, buffer, bufferLen);

    CompressOffsetReader offsetReader;
    ASSERT_NO_THROW(offsetReader.Init(buffer, bufferLen, docCount));

    ASSERT_TRUE(!offsetReader.IsU32Offset());

    for (size_t i = 0; i < offsetVec.size(); i++)
    {
        ASSERT_EQ(offsetVec[i], offsetReader.GetOffset((docid_t)i));
    }
    delete[] buffer;
}

void CompressOffsetReaderTest::TestInit()
{
    {
        // init success
        InnerTestInit<uint64_t>("0,3,5,7,9,10,12", 6, false, false, false);
        InnerTestInit<uint32_t>("0,3,5,7,9,10,12", 6, false, false, false);
    }

    {
        // init throw exception
        // invalid docCount
        InnerTestInit<uint32_t>("0,3,5,7,9,10,12", 5, false, false, true);

        // invalid magic tail
        InnerTestInit<uint32_t>("0,3,5,7,9,10,12", 6, true, false, true);

        // u32 not support expand
        InnerTestInit<uint32_t>("0,3,5,7,9,10,12", 6, false, true, true);
    }
}

void CompressOffsetReaderTest::TestSetOffset()
{
    DirectoryPtr segmentDirectory = GET_SEGMENT_DIRECTORY();
    SliceFilePtr sliceFile = segmentDirectory->CreateSliceFile(
            "expand_slice", 1024 * 1024, 10);
    SliceFileReaderPtr sliceFileReader = sliceFile->CreateSliceFileReader();

    string offsetStr = "0,3,5,7,9,10,12";
    vector<uint64_t> offsetVec;
    StringUtil::fromString(offsetStr, offsetVec, ",");
    uint8_t* buffer = NULL;
    size_t bufferLen;
    uint32_t docCount = offsetVec.size() - 1;
    PrepareCompressedOffsetBuffer<uint64_t>(offsetVec, buffer, bufferLen);

    CompressOffsetReader offsetReader;
    ASSERT_NO_THROW(offsetReader.Init(buffer, bufferLen, 
                    docCount, sliceFileReader));
    ASSERT_TRUE(!offsetReader.IsU32Offset());

    for (size_t i = 0; i < offsetVec.size(); i++)
    {
        offsetVec[i] = offsetVec[i] + 1000;
        ASSERT_TRUE(offsetReader.SetOffset(i, offsetVec[i]));
        ASSERT_EQ(offsetVec[i], offsetReader.GetOffset((docid_t)i));
    }
    delete[] buffer;
}

IE_NAMESPACE_END(index);

