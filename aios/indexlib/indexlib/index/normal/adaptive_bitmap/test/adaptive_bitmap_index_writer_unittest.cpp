#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/adaptive_bitmap/test/posting_iterator_mocker.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/on_disk_bitmap_index_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/test/index_test_util.h"

using namespace autil;
using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);

class AdaptiveBitmapIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AdaptiveBitmapIndexWriterTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void CheckGetAdaptiveDictKeys(
            const AdaptiveBitmapIndexWriterPtr& dictWriter,
            const vector<dictkey_t>& dictKeys)
    {
        vector<dictkey_t> result = dictWriter->GetAdaptiveDictKeys();
        for (size_t i = 0; i < dictKeys.size(); i++)
        {
            INDEXLIB_TEST_EQUAL(result[i], dictKeys[i]);
        }
    }

    void CheckAdaptiveBitmapIndex(vector<dictkey_t>& dictKeys,
                                  const vector<termpayload_t>& termPayloads,
                                  const vector<vector<docid_t> >& expectDocIdLists)
    {
        string adaptiveDictFilePath =
            storage::FileSystemWrapper::JoinPath(mRootDir, 
                    BITMAP_DICTIONARY_FILE_NAME);
        string adaptivePostingFilePath = 
            storage::FileSystemWrapper::JoinPath(mRootDir, BITMAP_POSTING_FILE_NAME);
        
        INDEXLIB_TEST_TRUE(
                storage::FileSystemWrapper::IsExist(adaptiveDictFilePath));
        INDEXLIB_TEST_TRUE(
                storage::FileSystemWrapper::IsExist(adaptivePostingFilePath));

        OnDiskBitmapIndexIteratorPtr ptr(
                new OnDiskBitmapIndexIterator(GET_PARTITION_DIRECTORY()));
        ptr->Init();
        
        dictkey_t key = 0;

        for (size_t i = 0; i < dictKeys.size(); i++)
        {
            BitmapPostingDecoder* decoder = 
                dynamic_cast<BitmapPostingDecoder*>(ptr->Next(key));
            const TermMeta *termMeta = decoder->GetTermMeta();
            INDEXLIB_TEST_EQUAL((df_t)(expectDocIdLists[i].size()), termMeta->GetDocFreq());
            INDEXLIB_TEST_EQUAL(termPayloads[i], termMeta->GetPayload());
            INDEXLIB_TEST_EQUAL(dictKeys[i], key);
            docid_t docBuffer[100];
            uint32_t decodedNum = decoder->DecodeDocList(docBuffer, 100);
            INDEXLIB_TEST_EQUAL(decodedNum, expectDocIdLists[i].size());
            for(size_t j = 0; j < decodedNum; j++)
            {
                INDEXLIB_TEST_EQUAL(expectDocIdLists[i][j], docBuffer[j]);
            }
        }
    }
    
    void CreateIndexDataWriters(BufferedFileWriterPtr& postingFile,
                                DictionaryWriterPtr& dictWriter)
    {
        string postingFileName = storage::FileSystemWrapper::JoinPath(
                mRootDir, BITMAP_POSTING_FILE_NAME);

        dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&mSimplePool));
        dictWriter->Open(GET_PARTITION_DIRECTORY(), BITMAP_DICTIONARY_FILE_NAME);

        postingFile.reset(new BufferedFileWriter());
        postingFile->Open(postingFileName);
    }
    
    void DestroyIndexDataWriters(BufferedFileWriterPtr& postingFile,
                                DictionaryWriterPtr& dictWriter)
    {
        dictWriter->Close();
        dictWriter.reset();

        postingFile->Close();
        postingFile.reset();
    }

    void TestCaseForAddPosting()
    {
        IndexTestUtil::ResetDir(mRootDir);

        BufferedFileWriterPtr postingFile;
        DictionaryWriterPtr dictWriter;
        CreateIndexDataWriters(postingFile, dictWriter);
        AdaptiveBitmapIndexWriterPtr adaptiveWriter =
            CreateAdaptiveBitmapIndexWriter();
        adaptiveWriter->Init(dictWriter, postingFile);

        vector<dictkey_t> expectDictKeys;
        vector<termpayload_t> expectTermPayloads;
        vector<vector<docid_t> > expectDocIdLists;
        AddAdaptiveBitmapPosting(adaptiveWriter, "1#2#1,2", 
                expectDictKeys, expectTermPayloads, expectDocIdLists);
        AddAdaptiveBitmapPosting(adaptiveWriter, "3#4#1,2,3", 
                expectDictKeys, expectTermPayloads, expectDocIdLists);
        // add empty posting list, not in adaptive bitmap
        AddAdaptiveBitmapPosting(adaptiveWriter, "3#4#", 
                expectDictKeys, expectTermPayloads, expectDocIdLists);
        DestroyIndexDataWriters(postingFile, dictWriter);
        CheckAdaptiveBitmapIndex(expectDictKeys, expectTermPayloads, expectDocIdLists);
        CheckGetAdaptiveDictKeys(adaptiveWriter, expectDictKeys);
    }

    void TestCaseForCollectDocIds()
    {
        vector<docid_t> docList;
        docList.push_back(1);
        docList.push_back(2);
        PostingIteratorPtr postingIter(
                new PostingIteratorMocker(docList));
        vector<docid_t> docIds;

        AdaptiveBitmapIndexWriterPtr adaptiveWriter;
        adaptiveWriter = CreateAdaptiveBitmapIndexWriter();
        adaptiveWriter->CollectDocIds(postingIter, docIds);
        INDEXLIB_TEST_EQUAL(1, docIds[0]);
        INDEXLIB_TEST_EQUAL(2, docIds[1]);
    }

    AdaptiveBitmapIndexWriterPtr CreateAdaptiveBitmapIndexWriter()
    {
        AdaptiveBitmapTriggerPtr trigger(new DfAdaptiveBitmapTrigger(10));
        config::IndexConfigPtr indexConfig(
                new config::SingleFieldIndexConfig("title", it_text));
        trigger->Init(indexConfig);
        ArchiveFolderPtr archiveFoler(new ArchiveFolder(false));
        archiveFoler->Open(mRootDir);
        return AdaptiveBitmapIndexWriterPtr(new AdaptiveBitmapIndexWriter(
                        trigger, indexConfig, archiveFoler));
    }

    // docString : termHashKey#termPayload#docid1,docid2,docid3...
    void AddAdaptiveBitmapPosting(
            const AdaptiveBitmapIndexWriterPtr& adaptiveWriter,
            const string& docInfoStr,
            vector<dictkey_t>& expectTermKeys,
            vector<termpayload_t>& expectTermPayloads,
            vector<vector<docid_t> >& expectDocLists)
    {
        vector<string> docInfos = StringUtil::split(docInfoStr, "#", false);
        assert(docInfos.size() == 3);
        dictkey_t dictKey;
        termpayload_t termPayload;
        vector<docid_t> docList;
        StringUtil::fromString(docInfos[0], dictKey);
        StringUtil::fromString(docInfos[1], termPayload);
        StringUtil::fromString(docInfos[2], docList, ",");

        PostingIteratorPtr postingIter(
                new PostingIteratorMocker(docList));
        adaptiveWriter->AddPosting(dictKey, termPayload, postingIter);
        if (docList.size() > 0)
        {
            expectTermKeys.push_back(dictKey);
            expectTermPayloads.push_back(termPayload);
            expectDocLists.push_back(docList);
        }
    }

private:
    string mRootDir;
    util::SimplePool mSimplePool;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, AdaptiveBitmapIndexWriterTest);

INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapIndexWriterTest, TestCaseForCollectDocIds);
INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapIndexWriterTest, TestCaseForAddPosting);

IE_NAMESPACE_END(index);
