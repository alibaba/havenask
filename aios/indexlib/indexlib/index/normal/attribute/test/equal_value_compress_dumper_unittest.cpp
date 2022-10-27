#include "indexlib/index/normal/attribute/test/equal_value_compress_dumper_unittest.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, EqualValueCompressDumperTest);

EqualValueCompressDumperTest::EqualValueCompressDumperTest()
{
}

EqualValueCompressDumperTest::~EqualValueCompressDumperTest()
{
}

void EqualValueCompressDumperTest::SetUp()
{
    mRootDir = FileSystemWrapper::JoinPath(
            TEST_DATA_PATH, "legacy_dfs_section_segment_reader_test");
    if (FileSystemWrapper::IsExist(mRootDir))
    {
        FileSystemWrapper::DeleteDir(mRootDir);
    }
    FileSystemWrapper::MkDir(mRootDir);
}

void EqualValueCompressDumperTest::TearDown()
{
    if (FileSystemWrapper::IsExist(mRootDir))
    {
        FileSystemWrapper::DeleteDir(mRootDir);
    }
}

void EqualValueCompressDumperTest::TestDumpMagicTail()
{
    {
        FileWriterPtr file = DirectoryCreator::Create(mRootDir)->CreateFileWriter(
            "uint32_magic_tail");
        EqualValueCompressDumper<uint32_t>::DumpMagicTail(file);
        file->Close();

        string fileContent;
        FileSystemWrapper::AtomicLoad(file->GetPath(), fileContent);
        
        ASSERT_EQ((size_t)4, fileContent.size());
        uint32_t magicTail = *(uint32_t*)(fileContent.data());
        ASSERT_EQ(UINT32_OFFSET_TAIL_MAGIC, magicTail);
    }

    {
        FileWriterPtr file = DirectoryCreator::Create(mRootDir)->CreateFileWriter(
            "uint64_magic_tail");
        EqualValueCompressDumper<uint64_t>::DumpMagicTail(file);
        file->Close();

        string fileContent;
        FileSystemWrapper::AtomicLoad(file->GetPath(), fileContent);
        
        ASSERT_EQ((size_t)4, fileContent.size());
        uint32_t magicTail = *(uint32_t*)(fileContent.data());
        ASSERT_EQ(UINT64_OFFSET_TAIL_MAGIC, magicTail);
    }
}

void EqualValueCompressDumperTest::TestDumpFile()
{
    // for var_num offset
    InnerTestDumpFile<uint32_t>(8371, true);
    InnerTestDumpFile<uint64_t>(7812, true);

    // for single attribute
    InnerTestDumpFile<uint32_t>(7812, false);
    InnerTestDumpFile<uint64_t>(7812, false);
    InnerTestDumpFile<uint16_t>(7812, false);
    InnerTestDumpFile<uint8_t>(7812, false);
    InnerTestDumpFile<int32_t>(7812, false);
    InnerTestDumpFile<int64_t>(7812, false);
    InnerTestDumpFile<int16_t>(7812, false);
    InnerTestDumpFile<int8_t>(7812, false);
}

IE_NAMESPACE_END(index);

