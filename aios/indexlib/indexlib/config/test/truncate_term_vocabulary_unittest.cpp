#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/truncate_term_vocabulary.h"
#include <autil/StringUtil.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_BEGIN(config);

class TruncateTermVocabularyTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateTermVocabularyTest);
    void CaseSetUp() override
    {
        string rootDir = GET_TEST_DATA_PATH();
        mFileName = FileSystemWrapper::JoinPath(rootDir, "truncate_meta");
    }

    void CaseTearDown() override
    {
    }
    
    
    void TestCaseForLoadTruncateTerms()
    {
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        TruncateTermVocabulary truncateTermVocabulary(folder);
        GenerateTruncateMetaFile(3, 2);
        FileWrapperPtr file = folder->GetInnerFile("truncate_meta", fslib::READ);
        truncateTermVocabulary.LoadTruncateTerms(file);
        INDEXLIB_TEST_EQUAL((size_t)3, truncateTermVocabulary.mTerms.size());
        INDEXLIB_TEST_TRUE(truncateTermVocabulary.Lookup(0));
        INDEXLIB_TEST_TRUE(truncateTermVocabulary.Lookup(1));
        INDEXLIB_TEST_TRUE(truncateTermVocabulary.Lookup(2));

        int32_t tf;
        INDEXLIB_TEST_TRUE(truncateTermVocabulary.LookupTF(0, tf));
        INDEXLIB_TEST_EQUAL(2, tf);
        INDEXLIB_TEST_TRUE(truncateTermVocabulary.LookupTF(1, tf));
        INDEXLIB_TEST_EQUAL(2, tf);
        INDEXLIB_TEST_TRUE(truncateTermVocabulary.LookupTF(2, tf));        
        INDEXLIB_TEST_EQUAL(2, tf);
    }

    void TestCaseForExtractTermKey()
    {
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        TruncateTermVocabulary truncateTermVocabulary(folder);
        {
            //normal case
            string keyStr = "12";
            string valueStr = "13";
            string line = keyStr + "\t" + valueStr + "\n";
            dictkey_t key = truncateTermVocabulary.ExtractTermKey(line);
            INDEXLIB_TEST_EQUAL((dictkey_t)12, key);
        }

        {
            //exception case
            string keyStr = "a";
            string valueStr = StringUtil::toString(13);
            string line = keyStr + "\t" + valueStr + "\n";
            bool hasException = false;
            try
            {
                truncateTermVocabulary.ExtractTermKey(line);
            }
            catch (const IndexCollapsedException& e)
            {
                hasException = true;
            }
            INDEXLIB_TEST_TRUE(hasException);
        }
    }


private:
    void GenerateTruncateMetaFile(uint32_t lineNum, uint32_t repeatedNum = 1)
    {
        //test for compatible case, index config test new case
        string content;
        for (uint32_t k = 0; k < repeatedNum; k++)
        {
            for (uint32_t i = 0; i < lineNum; i++)
            {
                dictkey_t key = i;
                int64_t value = rand();

                string keyStr = StringUtil::toString(key);
                string valueStr = StringUtil::toString(value);
                string line = keyStr + "\t" + valueStr + "\n";
                content += line;
            }
        }
        FileSystemWrapper::AtomicStore(mFileName, content);
    }
private:
    string mFileName;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateTermVocabularyTest);

INDEXLIB_UNIT_TEST_CASE(TruncateTermVocabularyTest, TestCaseForLoadTruncateTerms);
INDEXLIB_UNIT_TEST_CASE(TruncateTermVocabularyTest, TestCaseForExtractTermKey);

IE_NAMESPACE_END(config);
