#include "indexlib/index/inverted_index/truncate/TimeStrategyTruncateMetaReader.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TimeStrategyTruncateMetaReaderTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    void GenerateMetaFile(uint32_t lineNum, int64_t minTime, int64_t maxTime, std::map<dictkey_t, int64_t>& answer,
                          bool desc);
    void InnerTestForSimple(int64_t minTime, int64_t maxTime, bool desc);

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
};

void TimeStrategyTruncateMetaReaderTest::GenerateMetaFile(uint32_t lineNum, int64_t minTime, int64_t maxTime,
                                                          std::map<dictkey_t, int64_t>& answer, bool desc)
{
    std::string root = GET_TEMP_DATA_PATH();
    _rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(root);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    auto status = _rootDir->RemoveFile("truncate_meta", removeOption).Status();
    assert(status.IsOK());

    indexlib::file_system::WriterOption writerOption;
    auto writer = _rootDir->CreateFileWriter("truncate_meta", writerOption).Value();
    for (uint32_t i = 0; i < lineNum; ++i) {
        dictkey_t key = i;
        int64_t value = rand();

        std::string keyStr = autil::StringUtil::toString(key);
        std::string valueStr = autil::StringUtil::toString(value);
        std::string line = keyStr + "\t" + valueStr + "\n";
        auto status = writer->Write(line.c_str(), line.size()).Status();
        assert(status.IsOK());
        if (!desc) {
            if (value < maxTime) {
                value = maxTime;
            }
        } else {
            if (value > minTime) {
                value = minTime;
            }
        }
        answer[key] = value;
    }
    status = writer->Close().Status();
    ASSERT_TRUE(status.IsOK());
}

void TimeStrategyTruncateMetaReaderTest::InnerTestForSimple(int64_t minTime, int64_t maxTime, bool desc)
{
    std::map<dictkey_t, int64_t> answer;
    GenerateMetaFile(1000, minTime, maxTime, answer, desc);

    auto fileReader = _rootDir->CreateFileReader("truncate_meta", file_system::FSOT_BUFFERED).Value();
    auto reader = std::make_shared<TimeStrategyTruncateMetaReader>(minTime, maxTime, desc);
    auto status = reader->Open(fileReader);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(reader->Size(), answer.size());

    for (const auto& [key, value] : answer) {
        int64_t min, max;
        [[maybe_unused]] auto ret = reader->Lookup(index::DictKeyInfo(key), min, max);
        assert(ret);
        if (!desc) {
            ASSERT_EQ(minTime, min);
            ASSERT_EQ(value, max);
        } else {
            ASSERT_EQ(value, min);
            ASSERT_EQ(maxTime, max);
        }
    }
}

TEST_F(TimeStrategyTruncateMetaReaderTest, TestSimpleProcess)
{
    InnerTestForSimple(182735, 283716, true);
    InnerTestForSimple(18273531, 28371681, false);
}

} // namespace indexlib::index
