#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace testing;

namespace indexlib { namespace config {

class HighFrequencyVocabularyTest : public TESTBASE
{
public:
    void TestCaseForInit()
    {
        HighFrequencyVocabulary vocabulary;
        vocabulary.Init("test", it_pack, "a;an;of", {});
        ASSERT_EQ(string("test"), vocabulary.GetVocabularyName());
        ASSERT_TRUE(vocabulary.Lookup("a"));
        ASSERT_TRUE(vocabulary.Lookup("a"));
        ASSERT_TRUE(vocabulary.Lookup("an"));
        ASSERT_TRUE(vocabulary.Lookup("of"));
        ASSERT_TRUE(!vocabulary.Lookup("do"));
    }

    void TestCaseForNullTerm()
    {
        // null means NULL_TERM
        HighFrequencyVocabulary vocabulary("null");
        vocabulary.Init("test", it_number_int8, "1;2;3;null", {});
        ASSERT_TRUE(vocabulary.Lookup("1"));
        ASSERT_TRUE(vocabulary.Lookup("2"));
        ASSERT_TRUE(vocabulary.Lookup("3"));
        ASSERT_TRUE(vocabulary.Lookup("null"));

        ASSERT_TRUE(vocabulary.Lookup(index::DictKeyInfo::NULL_TERM));
        index::Term nullTerm;
        ASSERT_TRUE(vocabulary.Lookup(nullTerm));

        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(_testDir->GetIDirectory(), ""));
        ASSERT_TRUE(vocabulary.DumpTerms(folder).IsOK());
        ASSERT_EQ(FSEC_OK, folder->Close());

        ArchiveFolderPtr readFolder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, readFolder->Open(_testDir->GetIDirectory(), ""));
        auto dumpFile = readFolder->CreateFileReader("test").GetOrThrow();
        ASSERT_TRUE(dumpFile);
        string fileContent;
        fileContent.resize(dumpFile->GetLength());
        ASSERT_EQ(FSEC_OK, dumpFile->Read((void*)fileContent.data(), fileContent.size(), 0).Code());
        ASSERT_EQ(FSEC_OK, dumpFile->Close());
        ASSERT_EQ(FSEC_OK, readFolder->Close());

        vector<string> termHashStrVec;
        StringUtil::fromString(fileContent, termHashStrVec, "\n");
        EXPECT_THAT(termHashStrVec,
                    UnorderedElementsAre(string("3"), string("2"), string("1"), string("18446744073709551615:true")));
    }

    void TestCaseForAddKey()
    {
        {
            HighFrequencyVocabulary vocabulary;
            vocabulary.Init("test", it_pack, "a;an;of", {});
            vocabulary.AddKey(index::DictKeyInfo(16));
            ASSERT_TRUE(vocabulary.Lookup(index::DictKeyInfo(16)));
            ASSERT_TRUE(vocabulary.Lookup("a"));
            ASSERT_TRUE(vocabulary.Lookup("an"));
            ASSERT_TRUE(vocabulary.Lookup("of"));
            ASSERT_TRUE(!vocabulary.Lookup("do"));
        }
        {
            HighFrequencyVocabulary vocabulary;
            vocabulary.Init("test", it_string, "a;an#3;of#1", {{"dict_hash_func", "LayerHash"}});
            vocabulary.AddKey(index::DictKeyInfo(16));
            ASSERT_TRUE(vocabulary.Lookup(index::DictKeyInfo(16)));
            ASSERT_TRUE(vocabulary.Lookup("a"));
            ASSERT_TRUE(vocabulary.Lookup("an#3"));
            ASSERT_TRUE(vocabulary.Lookup("of#1"));
            ASSERT_TRUE(!vocabulary.Lookup("do"));
        }
    }

    void TestCaseForDumpTerms()
    {
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(_testDir->GetIDirectory(), ""));
        HighFrequencyVocabulary vocabulary;
        vocabulary.Init("test", it_number_int8, "1;2;3", {});
        ASSERT_TRUE(vocabulary.DumpTerms(folder).IsOK());
        ASSERT_EQ(FSEC_OK, folder->Close());
        ArchiveFolderPtr readFolder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, readFolder->Open(_testDir->GetIDirectory(), ""));
        auto dumpFile = readFolder->CreateFileReader("test").GetOrThrow();
        ASSERT_TRUE(dumpFile);

        unique_ptr<char[]> buffer(new char[dumpFile->GetLength()]);
        ASSERT_EQ(FSEC_OK, dumpFile->Read(buffer.get(), dumpFile->GetLength()).Code());
        string fileContent(buffer.get(), dumpFile->GetLength());
        ASSERT_EQ(FSEC_OK, dumpFile->Close());
        ASSERT_EQ(FSEC_OK, readFolder->Close());

        vector<string> termHashStrVec;
        StringUtil::fromString(fileContent, termHashStrVec, "\n");
        EXPECT_THAT(termHashStrVec, UnorderedElementsAre(string("3"), string("1"), string("2")));
    }

    void setUp() override { _testDir = Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH()); }

private:
    shared_ptr<Directory> _testDir;
};

TEST_F(HighFrequencyVocabularyTest, TestCaseForInit) { TestCaseForInit(); }
TEST_F(HighFrequencyVocabularyTest, TestCaseForAddKey) { TestCaseForAddKey(); }
TEST_F(HighFrequencyVocabularyTest, TestCaseForDumpTerms) { TestCaseForDumpTerms(); }
TEST_F(HighFrequencyVocabularyTest, TestCaseForNullTerm) { TestCaseForNullTerm(); }
}} // namespace indexlib::config
