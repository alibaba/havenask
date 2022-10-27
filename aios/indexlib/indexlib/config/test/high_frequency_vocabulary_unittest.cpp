#include "indexlib/test/test.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/high_frequency_vocabulary.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(config);

class HighFrequencyVocabularyTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(HighFrequencyVocabularyTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForInit()
    {
        HighFrequencyVocabulary vocabulary;
        vocabulary.Init("test", it_pack, "a;an;of");
        ASSERT_EQ(string("test"), vocabulary.GetVocabularyName());
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("a"));
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("a"));
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("an"));
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("of"));
        INDEXLIB_TEST_TRUE(!vocabulary.Lookup("do"));
    }

    void TestCaseForAddKey()
    {
        HighFrequencyVocabulary vocabulary;

        vocabulary.Init("test", it_pack, "a;an;of");
        vocabulary.AddKey(16);
        INDEXLIB_TEST_TRUE(vocabulary.Lookup(16));
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("a"));
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("an"));
        INDEXLIB_TEST_TRUE(vocabulary.Lookup("of"));
        INDEXLIB_TEST_TRUE(!vocabulary.Lookup("do"));
    }

    void TestCaseForDumpTerms()
    {
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        HighFrequencyVocabulary vocabulary;
        vocabulary.Init("test", it_number_int8, "1;2;3");
        vocabulary.DumpTerms(folder);
        folder->Close();
        ArchiveFolderPtr readFolder(new ArchiveFolder(true));
        readFolder->Open(GET_TEST_DATA_PATH());
        FileWrapperPtr dumpFile = readFolder->GetInnerFile("test", fslib::READ);
        ASSERT_TRUE(dumpFile);
        string fileContent;
        FileSystemWrapper::AtomicLoad(dumpFile.get(), fileContent);
        dumpFile->Close();
        readFolder->Close();
        string expectString("1\n2\n3\n");
        ASSERT_EQ(fileContent, expectString);
    }


private:
};

INDEXLIB_UNIT_TEST_CASE(HighFrequencyVocabularyTest, TestCaseForInit);
INDEXLIB_UNIT_TEST_CASE(HighFrequencyVocabularyTest, TestCaseForAddKey);
INDEXLIB_UNIT_TEST_CASE(HighFrequencyVocabularyTest, TestCaseForDumpTerms);

IE_NAMESPACE_END(config);
