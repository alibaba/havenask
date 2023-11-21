#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/config/truncate_term_vocabulary.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;
;
namespace indexlib { namespace config {

class TruncateTermVocabularyTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateTermVocabularyTest);
    void CaseSetUp() override
    {
        mRootDir = IDirectory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
    }

    void CaseTearDown() override {}

    void TestCaseForLoadTruncateTerms()
    {
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        TruncateTermVocabulary truncateTermVocabulary(folder);
        GenerateTruncateMetaFile(3, 2);
        auto file = folder->CreateFileReader("truncate_meta").GetOrThrow();
        truncateTermVocabulary.LoadTruncateTerms(file);
        // 3lines + nullterm
        ASSERT_EQ((size_t)(3 + 1), truncateTermVocabulary.GetTermCount());
        ASSERT_TRUE(truncateTermVocabulary.Lookup(index::DictKeyInfo(0)));
        ASSERT_TRUE(truncateTermVocabulary.Lookup(index::DictKeyInfo(1)));
        ASSERT_TRUE(truncateTermVocabulary.Lookup(index::DictKeyInfo(2)));
        ASSERT_TRUE(truncateTermVocabulary.Lookup(index::DictKeyInfo::NULL_TERM));

        int32_t tf;
        ASSERT_TRUE(truncateTermVocabulary.LookupTF(index::DictKeyInfo(0), tf));
        ASSERT_EQ(2, tf);
        ASSERT_TRUE(truncateTermVocabulary.LookupTF(index::DictKeyInfo(1), tf));
        ASSERT_EQ(2, tf);
        ASSERT_TRUE(truncateTermVocabulary.LookupTF(index::DictKeyInfo(2), tf));
        ASSERT_EQ(2, tf);

        ASSERT_TRUE(truncateTermVocabulary.LookupTF(index::DictKeyInfo::NULL_TERM, tf));
        ASSERT_EQ(2, tf);
    }

    void TestCaseForExtractTermKey()
    {
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        TruncateTermVocabulary truncateTermVocabulary(folder);
        {
            // normal case
            string keyStr = "12";
            string valueStr = "13";
            string line = keyStr + "\t" + valueStr + "\n";
            dictkey_t key = truncateTermVocabulary.ExtractTermKey(line).GetKey();
            ASSERT_EQ((dictkey_t)12, key);
        }

        {
            // null term case
            string keyStr = index::DictKeyInfo::NULL_TERM.ToString();
            string valueStr = "13";
            string line = keyStr + "\t" + valueStr + "\n";
            index::DictKeyInfo key = truncateTermVocabulary.ExtractTermKey(line);
            ASSERT_EQ(index::DictKeyInfo::NULL_TERM, key);
        }

        {
            // exception case
            string keyStr = "a";
            string valueStr = StringUtil::toString(13);
            string line = keyStr + "\t" + valueStr + "\n";
            bool hasException = false;
            try {
                truncateTermVocabulary.ExtractTermKey(line);
            } catch (const BadParameterException& e) {
                hasException = true;
            }
            ASSERT_TRUE(hasException);
        }
    }

private:
    void GenerateTruncateMetaFile(uint32_t lineNum, uint32_t repeatedNum = 1)
    {
        // test for compatible case, index config test new case
        string content;
        for (uint32_t k = 0; k < repeatedNum; k++) {
            for (uint32_t i = 0; i < lineNum; i++) {
                dictkey_t key = i;
                int64_t value = rand();

                string keyStr = StringUtil::toString(key);
                string valueStr = StringUtil::toString(value);
                string line = keyStr + "\t" + valueStr + "\n";
                content += line;
            }

            string line = index::DictKeyInfo::NULL_TERM.ToString() + "\t" + StringUtil::toString(rand()) + "\n";
            content += line;
        }
        mRootDir->Store("truncate_meta", content, {}).GetOrThrow();
    }

private:
    string mFileName;
    std::shared_ptr<file_system::IDirectory> mRootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(index, TruncateTermVocabularyTest);

INDEXLIB_UNIT_TEST_CASE(TruncateTermVocabularyTest, TestCaseForLoadTruncateTerms);
INDEXLIB_UNIT_TEST_CASE(TruncateTermVocabularyTest, TestCaseForExtractTermKey);
}} // namespace indexlib::config
