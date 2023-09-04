#include "indexlib/index/kkv/dump/KKVValueDumper.h"

#include "autil/MultiValueType.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index {

class KKVValueDumperTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("KKVValueDumperTest", indexlib::file_system::DirectoryOption())
                         ->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
    }

protected:
    void CheckData(const string& str);

protected:
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
};

TEST_F(KKVValueDumperTest, testVarLenValue)
{
    int32_t fixedLen = -1;
    KKVValueDumper dumper(fixedLen);
    auto writerOption = indexlib::file_system::WriterOption::Buffer();
    ASSERT_TRUE(dumper.Init(_directory, writerOption).IsOK());

    string value0 = "123";
    auto [status0, valueOffset0] = dumper.Dump(value0);
    ASSERT_TRUE(status0.IsOK());
    ASSERT_EQ(0, valueOffset0.chunkOffset);
    ASSERT_EQ(0, valueOffset0.inChunkOffset);
    size_t headSize = 1;
    ASSERT_EQ(value0.size() + headSize, dumper.GetMaxValueLen());

    string value1 = "4567";
    auto [status1, valueOffset1] = dumper.Dump(value1);
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ(0, valueOffset1.chunkOffset);
    ASSERT_EQ(value0.size() + headSize, valueOffset1.inChunkOffset);
    ASSERT_EQ(value1.size() + headSize, dumper.GetMaxValueLen());

    ASSERT_TRUE(dumper.Close().IsOK());
    CheckData("9:123,4567");
}

TEST_F(KKVValueDumperTest, testFixedLenValue)
{
    int32_t fixedLen = 4;
    KKVValueDumper dumper(fixedLen);
    auto writerOption = indexlib::file_system::WriterOption::Buffer();
    ASSERT_TRUE(dumper.Init(_directory, writerOption).IsOK());

    ASSERT_EQ(fixedLen, dumper.GetMaxValueLen());

    string value0 = string("\3") + "123";
    auto [status0, valueOffset0] = dumper.Dump(value0);
    ASSERT_TRUE(status0.IsOK());
    ASSERT_EQ(0, valueOffset0.chunkOffset);
    ASSERT_EQ(0, valueOffset0.inChunkOffset);

    string value1 = string("\3") + "456";
    auto [status1, valueOffset1] = dumper.Dump(value1);
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ(0, valueOffset1.chunkOffset);
    ASSERT_EQ(fixedLen, valueOffset1.inChunkOffset);

    ASSERT_TRUE(dumper.Close().IsOK());
    CheckData("8:123,456");
}

TEST_F(KKVValueDumperTest, testLegacyUT)
{
    int32_t fixedLen = -1;
    KKVValueDumper dumper(fixedLen);

    auto writerOption = indexlib::file_system::WriterOption::Buffer();
    ASSERT_TRUE(dumper.Init(_directory, writerOption).IsOK());

    string value0("abc");
    auto [status0, offset0] = dumper.Dump(value0);
    ASSERT_TRUE(status0.IsOK());
    string value1("efg");
    auto [status1, offset1] = dumper.Dump(value1);
    ASSERT_TRUE(status1.IsOK());
    ASSERT_TRUE(dumper._chunkWriter->TEST_ForceFlush());
    string value2("higk");
    auto [status2, offset2] = dumper.Dump(value2);
    ASSERT_TRUE(status2.IsOK());
    ASSERT_TRUE(dumper.Close().IsOK());

    CheckData("8:abc,efg;5:higk");
}

void KKVValueDumperTest::CheckData(const string& str)
{
    vector<vector<string>> dataInfoStrs;
    StringUtil::fromString(str, dataInfoStrs, ":", ";");

    auto fileReader = _directory->CreateFileReader(KKV_VALUE_FILE_NAME, indexlib::file_system::FSOT_MEM).GetOrThrow();
    size_t totalLen = 0;
    for (size_t i = 0; i < dataInfoStrs.size(); i++) {
        // align to 8 bytes
        totalLen = (totalLen + 7) / 8 * 8;
        ASSERT_TRUE(dataInfoStrs[i].size() == 2);
        indexlibv2::index::ChunkMeta meta;
        ASSERT_EQ(sizeof(meta), fileReader->Read(&meta, sizeof(meta), totalLen).GetOrThrow());
        totalLen += sizeof(meta);
        ASSERT_EQ(StringUtil::fromString<uint32_t>(dataInfoStrs[i][0]), meta.length);

        string valueStr;
        valueStr.resize(meta.length);
        ASSERT_EQ((size_t)meta.length,
                  fileReader->Read((void*)valueStr.data(), (size_t)meta.length, totalLen).GetOrThrow());

        size_t cursor = 0;
        vector<string> values;
        StringUtil::fromString(dataInfoStrs[i][1], values, ",");
        for (size_t j = 0; j < values.size(); j++) {
            autil::MultiChar mc(valueStr.c_str() + cursor);
            ASSERT_EQ(values[j].size(), mc.size());
            ASSERT_EQ(values[j], string(mc.data(), mc.size()));
            cursor += mc.length();
        }
        totalLen += meta.length;
    }
    ASSERT_EQ(totalLen, fileReader->GetLength());
}
} // namespace indexlibv2::index
