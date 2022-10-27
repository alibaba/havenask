#include "indexlib/config/test/high_freq_vocabulary_creator_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, HighFreqVocabularyCreatorTest);

HighFreqVocabularyCreatorTest::HighFreqVocabularyCreatorTest()
{
}

HighFreqVocabularyCreatorTest::~HighFreqVocabularyCreatorTest()
{
}

void HighFreqVocabularyCreatorTest::CaseSetUp()
{
}

void HighFreqVocabularyCreatorTest::CaseTearDown()
{
}

void HighFreqVocabularyCreatorTest::TestCreateVocabulary()
{
    {
        //test no dict config
        ASSERT_FALSE(HighFreqVocabularyCreator::CreateVocabulary(
                        "index", it_text, DictionaryConfigPtr()));
    }
    {
        DictionaryConfigPtr dictConfig(new DictionaryConfig("test", "1;2;3"));
        HighFrequencyVocabularyPtr vol = 
            HighFreqVocabularyCreator::CreateVocabulary(
                    "test", it_number_int8, dictConfig);
        ASSERT_EQ(string("test"), vol->GetVocabularyName());
        ASSERT_TRUE(vol->Lookup(1));
        ASSERT_TRUE(vol->Lookup(2));
        ASSERT_TRUE(vol->Lookup(3));
        ASSERT_FALSE(vol->Lookup(4));
    }
}

void HighFreqVocabularyCreatorTest::TestLoadAdaptiveVocabularyWithDir()
{
    {
        //test load with dict config
        TearDown();
        SetUp();
        DictionaryConfigPtr dictConfig(new DictionaryConfig("dict", "4;5"));
        PrepareAdaptiveDictFile("index_dict", false);
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        HighFrequencyVocabularyPtr volPtr = 
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                    folder, "index", it_number_int8, dictConfig);
        folder->Close();
        ASSERT_TRUE(volPtr->Lookup(0));
        ASSERT_TRUE(volPtr->Lookup(1));
        ASSERT_TRUE(volPtr->Lookup(2));
        ASSERT_FALSE(volPtr->Lookup(3));
        ASSERT_TRUE(volPtr->Lookup(4));
        ASSERT_TRUE(volPtr->Lookup(5));
    }

    {
        //test load without dict config
        TearDown();
        SetUp();
        DictionaryConfigPtr dictConfig;
        PrepareAdaptiveDictFile("index");
        ArchiveFolderPtr folder(new ArchiveFolder(true));
        folder->Open(GET_TEST_DATA_PATH());
        HighFrequencyVocabularyPtr volPtr = 
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                    folder, "index", it_number_int8, dictConfig);
        folder->Close();
        ASSERT_TRUE(volPtr->Lookup(0));
        ASSERT_TRUE(volPtr->Lookup(1));
        ASSERT_TRUE(volPtr->Lookup(2));
        ASSERT_FALSE(volPtr->Lookup(3));
    }

    {
        //test not exist adaptive dir
        DictionaryConfigPtr dictConfig(new DictionaryConfig("dict", "4;5"));
        ArchiveFolderPtr folder;
        HighFrequencyVocabularyPtr volPtr = 
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                    folder, "index", it_number_int8, dictConfig);
        ASSERT_FALSE(volPtr);
    }

    {
        //test load with dict file not exist
        TearDown();
        SetUp();
        DictionaryConfigPtr dictConfig(new DictionaryConfig("dict", "4;5"));

        PrepareAdaptiveDictFile("index");
        ArchiveFolderPtr folder(new ArchiveFolder(false));
        folder->Open(GET_TEST_DATA_PATH());
        HighFrequencyVocabularyPtr volPtr = 
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                    folder, "index", it_number_int8, dictConfig);
        ASSERT_FALSE(volPtr);
        folder->Close();
    }
}

void HighFreqVocabularyCreatorTest::PrepareAdaptiveDictFile(
        const string& fileName, bool useArchiveFile)
{
    stringstream ss;
    ss << 0 << endl;
    ss << 1 << endl;
    ss << 2;
    ArchiveFolder folder(!useArchiveFile);
    folder.Open(GET_TEST_DATA_PATH());
    FileWrapperPtr file = folder.GetInnerFile(fileName, fslib::WRITE);
    string content = ss.str();
    file->Write(content.c_str(), content.size());
    file->Close();
    folder.Close();
}

IE_NAMESPACE_END(config);

