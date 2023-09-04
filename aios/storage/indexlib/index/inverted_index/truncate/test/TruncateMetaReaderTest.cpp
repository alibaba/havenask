#include "indexlib/index/inverted_index/truncate/TruncateMetaReader.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TruncateMetaReaderTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    void GenerateMetaFile(uint32_t number, std::map<dictkey_t, int64_t>& meta);

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
};

void TruncateMetaReaderTest::GenerateMetaFile(uint32_t number, std::map<dictkey_t, int64_t>& meta)
{
    std::string root = GET_TEMP_DATA_PATH();
    _rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(root);
    indexlib::file_system::WriterOption writerOption;
    auto writer = _rootDir->CreateFileWriter("truncate_meta", writerOption).Value();
    for (uint32_t i = 0; i < number; ++i) {
        dictkey_t key = i;
        int64_t value = rand();

        std::string keyStr = autil::StringUtil::toString(key);
        std::string valueStr = autil::StringUtil::toString(value);
        std::string line = keyStr + "\t" + valueStr + "\n";
        auto status = writer->Write(line.c_str(), line.size()).Status();
        assert(status.IsOK());
        meta[key] = value;
    }
    auto st = writer->Close().Status();
    assert(st.IsOK());
}

TEST_F(TruncateMetaReaderTest, TestSimpleProcess)
{
    std::map<dictkey_t, int64_t> meta;
    GenerateMetaFile(1000, meta);

    auto fileReader = _rootDir->CreateFileReader("truncate_meta", file_system::FSOT_BUFFERED).Value();
    auto reader = std::make_shared<TruncateMetaReader>(true);
    auto status = reader->Open(fileReader);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(reader->Size(), meta.size());

    auto expectedMax = std::numeric_limits<int64_t>::max();
    for (const auto& [key, value] : meta) {
        int64_t min, max;
        auto ret = reader->Lookup(index::DictKeyInfo(key), min, max);
        ASSERT_TRUE(ret);
        ASSERT_EQ(value, min);
        ASSERT_EQ(expectedMax, max);
    }
}
} // namespace indexlib::index
