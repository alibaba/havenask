#include "indexlib/index/inverted_index/config/TruncateTermVocabulary.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TruncateTermVocabularyTest : public TESTBASE
{
public:
    void setUp() override
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        std::string indexRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("truncate_meta", indexRoot, fsOptions).GetOrThrow();
        auto indexDir = indexlib::file_system::Directory::Get(fs);
        _indexDir = indexDir->GetIDirectory();
    }
    void tearDown() override {}

private:
    void GenerateTruncateMetaFile(uint32_t lineNum, uint32_t repeatedNum);

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _indexDir;
};

void TruncateTermVocabularyTest::GenerateTruncateMetaFile(uint32_t lineNum, uint32_t repeatedNum)
{
    std::string content;
    for (uint32_t k = 0; k < repeatedNum; ++k) {
        for (uint32_t i = 0; i < lineNum; ++i) {
            dictkey_t key = i;
            int64_t value = rand();

            std::string keyStr = autil::StringUtil::toString(key);
            std::string valueStr = autil::StringUtil::toString(value);
            std::string line = keyStr + "\t" + valueStr + "\n";
            content += line;
        }

        std::string line =
            indexlib::index::DictKeyInfo::NULL_TERM.ToString() + "\t" + autil::StringUtil::toString(rand()) + "\n";
        content += line;
    }
    auto st = _indexDir->Store("truncate_meta", content, indexlib::file_system::WriterOption::AtomicDump()).Status();
    assert(st.IsOK());
}

TEST_F(TruncateTermVocabularyTest, TestSimpleProcess)
{
    std::shared_ptr<indexlib::file_system::ArchiveFolder> folder(new indexlib::file_system::ArchiveFolder(true));
    auto st = folder->Open(_indexDir, "").Status();
    ASSERT_TRUE(st.IsOK());
    TruncateTermVocabulary truncateTermVocabulary(folder);
    GenerateTruncateMetaFile(3, 2);
    auto file = folder->CreateFileReader("truncate_meta").GetOrThrow();
    st = truncateTermVocabulary.LoadTruncateTerms(file);
    ASSERT_TRUE(st.IsOK());
    // 3lines + nullterm
    ASSERT_EQ((size_t)(3 + 1), truncateTermVocabulary.GetTermCount());
    ASSERT_TRUE(truncateTermVocabulary.Lookup(indexlib::index::DictKeyInfo(0)));
    ASSERT_TRUE(truncateTermVocabulary.Lookup(indexlib::index::DictKeyInfo(1)));
    ASSERT_TRUE(truncateTermVocabulary.Lookup(indexlib::index::DictKeyInfo(2)));
    ASSERT_TRUE(truncateTermVocabulary.Lookup(indexlib::index::DictKeyInfo::NULL_TERM));

    int32_t tf;
    ASSERT_TRUE(truncateTermVocabulary.LookupTF(indexlib::index::DictKeyInfo(0), tf));
    ASSERT_EQ(2, tf);
    ASSERT_TRUE(truncateTermVocabulary.LookupTF(indexlib::index::DictKeyInfo(1), tf));
    ASSERT_EQ(2, tf);
    ASSERT_TRUE(truncateTermVocabulary.LookupTF(indexlib::index::DictKeyInfo(2), tf));
    ASSERT_EQ(2, tf);

    ASSERT_TRUE(truncateTermVocabulary.LookupTF(indexlib::index::DictKeyInfo::NULL_TERM, tf));
    ASSERT_EQ(2, tf);
}

} // namespace indexlibv2::config
