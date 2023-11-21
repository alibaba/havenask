#include "indexlib/config/test/high_freq_vocabulary_creator_unittest.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace config {

AUTIL_LOG_SETUP(indexlib.config, HighFreqVocabularyCreatorTest);

HighFreqVocabularyCreatorTest::HighFreqVocabularyCreatorTest() {}

HighFreqVocabularyCreatorTest::~HighFreqVocabularyCreatorTest() {}

void HighFreqVocabularyCreatorTest::CaseSetUp()
{
    mRootDir = IDirectory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
}

void HighFreqVocabularyCreatorTest::CaseTearDown() {}

void HighFreqVocabularyCreatorTest::TestCreateVocabulary()
{
    {
        // test no dict config
        ASSERT_FALSE(
            HighFreqVocabularyCreator::CreateVocabulary("index", it_text, std::shared_ptr<DictionaryConfig>(), "", {}));
    }
    {
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig("test", "1;2;3;null"));
        std::shared_ptr<HighFrequencyVocabulary> vol =
            HighFreqVocabularyCreator::CreateVocabulary("test", it_number_int8, dictConfig, "null", {});
        ASSERT_EQ(string("test"), vol->GetVocabularyName());
        ASSERT_TRUE(vol->Lookup(index::DictKeyInfo(1)));
        ASSERT_TRUE(vol->Lookup(index::DictKeyInfo(2)));
        ASSERT_TRUE(vol->Lookup(index::DictKeyInfo(3)));
        ASSERT_TRUE(vol->Lookup(index::DictKeyInfo::NULL_TERM));
        ASSERT_FALSE(vol->Lookup(index::DictKeyInfo(4)));
    }
    {
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig("test", "1#1;1#4;2#1;1#3;null"));
        std::shared_ptr<HighFrequencyVocabulary> vol = HighFreqVocabularyCreator::CreateVocabulary(
            "test", it_text, dictConfig, "null", {{"dict_hash_func", "LayerHash"}});
        ASSERT_EQ(string("test"), vol->GetVocabularyName());
        ASSERT_TRUE(vol->Lookup("1#1"));
        ASSERT_TRUE(vol->Lookup("2#1"));
        ASSERT_TRUE(vol->Lookup("1#3"));
        ASSERT_TRUE(vol->Lookup("1#4"));
        ASSERT_TRUE(vol->Lookup(index::DictKeyInfo::NULL_TERM));
        ASSERT_FALSE(vol->Lookup("4"));

        string word = "1#3";
        dictkey_t hashKey;
        index::KeyHasherWrapper::GetHashKeyByInvertedIndexType(it_text, word.c_str(), word.size(), hashKey);
        ASSERT_FALSE(vol->Lookup(index::DictKeyInfo(hashKey)));
    }
}

void HighFreqVocabularyCreatorTest::TestLoadAdaptiveVocabularyWithDir()
{
    {
        // test load with dict config
        tearDown();
        setUp();

        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig("dict", "4;5"));
        PrepareAdaptiveDictFile("index_dict", false);
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        std::shared_ptr<HighFrequencyVocabulary> volPtr =
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(folder, "index", it_number_int8, "", dictConfig, {})
                .GetOrThrow();
        ASSERT_EQ(FSEC_OK, folder->Close());
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(0)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(1)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(2)));
        ASSERT_FALSE(volPtr->Lookup(index::DictKeyInfo(3)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(4)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(5)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo::NULL_TERM));
    }

    {
        // test load without dict config
        tearDown();
        setUp();
        std::shared_ptr<DictionaryConfig> dictConfig;
        PrepareAdaptiveDictFile("index");
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        std::shared_ptr<HighFrequencyVocabulary> volPtr =
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(folder, "index", it_number_int8, "", dictConfig, {})
                .GetOrThrow();
        ASSERT_EQ(FSEC_OK, folder->Close());
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(0)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(1)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo(2)));
        ASSERT_TRUE(volPtr->Lookup(index::DictKeyInfo::NULL_TERM));
        ASSERT_FALSE(volPtr->Lookup(index::DictKeyInfo(3)));
    }

    {
        // test not exist adaptive dir
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig("dict", "4;5"));
        ArchiveFolderPtr folder;
        std::shared_ptr<HighFrequencyVocabulary> volPtr =
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(folder, "index", it_number_int8, "", dictConfig, {})
                .GetOrThrow();
        ASSERT_FALSE(volPtr);
    }

    {
        // test load with dict file not exist
        tearDown();
        setUp();
        std::shared_ptr<DictionaryConfig> dictConfig(new DictionaryConfig("dict", "4;5"));

        PrepareAdaptiveDictFile("index");
        ArchiveFolderPtr folder(new ArchiveFolder(false));
        ASSERT_EQ(FSEC_OK, folder->Open(mRootDir, ""));
        std::shared_ptr<HighFrequencyVocabulary> volPtr =
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(folder, "index", it_number_int8, "", dictConfig, {})
                .GetOrThrow();
        ASSERT_FALSE(volPtr);
        ASSERT_EQ(FSEC_OK, folder->Close());
    }
}

void HighFreqVocabularyCreatorTest::PrepareAdaptiveDictFile(const string& fileName, bool useArchiveFile)
{
    stringstream ss;
    ss << 0 << endl;
    ss << 1 << endl;
    ss << 2 << endl;
    ss << index::DictKeyInfo::NULL_TERM.ToString() << endl;
    ArchiveFolder folder(!useArchiveFile);
    ASSERT_EQ(FSEC_OK, folder.Open(mRootDir, ""));
    auto file = folder.CreateFileWriter(fileName).GetOrThrow();
    string content = ss.str();
    file->Write(content.c_str(), content.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, file->Close());
    ASSERT_EQ(FSEC_OK, folder.Close());
}
}} // namespace indexlib::config
