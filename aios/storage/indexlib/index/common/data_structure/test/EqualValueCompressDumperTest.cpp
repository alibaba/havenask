#include "indexlib/index/common/data_structure/EqualValueCompressDumper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/data_structure/EqualValueCompressAdvisor.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class EqualValueCompressDumperTest : public TESTBASE
{
public:
    EqualValueCompressDumperTest() = default;
    ~EqualValueCompressDumperTest() = default;

public:
    void setUp() override;
    void tearDown() override;

    template <typename T>
    void MakeData(uint32_t count, std::vector<T>& valueArray)
    {
        for (uint32_t i = 0; i < count; ++i) {
            T value = (T)rand();
            valueArray.push_back(value);
        }
    }

    template <typename T>
    void InnerTestDumpFile(uint32_t count, bool needMagicTail)
    {
        tearDown();
        setUp();

        std::vector<T> data;
        MakeData<T>(1234, data);
        autil::mem_pool::Pool pool;
        auto file = _rootDir->CreateFileWriter("dumpFile_test");
        std::string filePath = indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), file->GetLogicalPath());
        EqualValueCompressDumper<T> dumper(needMagicTail, &pool);
        dumper.CompressData(data);
        ASSERT_TRUE(dumper.Dump(file).IsOK());
        ASSERT_EQ(indexlib::file_system::FSEC_OK, file->Close());

        std::string content;
        indexlib::file_system::FslibWrapper::AtomicLoadE(filePath, content);
        indexlib::index::EquivalentCompressReader<T> reader((uint8_t*)content.data());
        size_t expectedFileLen = 0;
        EqualValueCompressAdvisor<T>::GetOptSlotItemCount(reader, expectedFileLen);
        if (needMagicTail) {
            expectedFileLen += sizeof(uint32_t); // magicTail
        }

        size_t fileLen = indexlib::file_system::FslibWrapper::GetFileLength(filePath).GetOrThrow();
        indexlib::file_system::FslibWrapper::DeleteFileE(filePath, indexlib::file_system::DeleteOption::NoFence(false));
        ASSERT_EQ(expectedFileLen, fileLen);

        ASSERT_EQ(data.size(), (size_t)reader.Size());
        for (size_t i = 0; i < data.size(); i++) {
            ASSERT_EQ(data[i], reader[i].second);
        }
    }

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

void EqualValueCompressDumperTest::setUp()
{
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}

void EqualValueCompressDumperTest::tearDown() {}

TEST_F(EqualValueCompressDumperTest, TestDumpMagicTail)
{
    {
        auto file = _rootDir->CreateFileWriter("uint32_magic_tail");
        ASSERT_TRUE(EqualValueCompressDumper<uint32_t>::DumpMagicTail(file).IsOK());
        ASSERT_EQ(indexlib::file_system::FSEC_OK, file->Close());

        std::string fileContent;
        indexlib::file_system::FslibWrapper::AtomicLoadE(
            indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), file->GetLogicalPath()), fileContent);

        ASSERT_EQ((size_t)4, fileContent.size());
        uint32_t magicTail = *(uint32_t*)(fileContent.data());
        ASSERT_EQ(UINT32_OFFSET_TAIL_MAGIC, magicTail);
    }
    tearDown();
    setUp();
    {
        auto file = _rootDir->CreateFileWriter("uint64_magic_tail");
        ASSERT_TRUE(EqualValueCompressDumper<uint64_t>::DumpMagicTail(file).IsOK());
        ASSERT_EQ(indexlib::file_system::FSEC_OK, file->Close());

        std::string fileContent;
        indexlib::file_system::FslibWrapper::AtomicLoadE(
            indexlib::util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), file->GetLogicalPath()), fileContent);

        ASSERT_EQ((size_t)4, fileContent.size());
        uint32_t magicTail = *(uint32_t*)(fileContent.data());
        ASSERT_EQ(UINT64_OFFSET_TAIL_MAGIC, magicTail);
    }
}

TEST_F(EqualValueCompressDumperTest, TestDumpFile)
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

} // namespace indexlibv2::index
