#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/test/posting_iterator_mocker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace autil;
using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::file_system;

namespace indexlib { namespace index { namespace legacy {

class AdaptiveBitmapIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AdaptiveBitmapIndexWriterTest);
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void CheckGetAdaptiveDictKeys(const AdaptiveBitmapIndexWriterPtr& dictWriter,
                                  const vector<index::DictKeyInfo>& dictKeys)
    {
        vector<index::DictKeyInfo> result = dictWriter->GetAdaptiveDictKeys();
        for (size_t i = 0; i < dictKeys.size(); i++) {
            INDEXLIB_TEST_EQUAL(result[i], dictKeys[i]);
        }
    }

    void CheckAdaptiveBitmapIndex(vector<index::DictKeyInfo>& dictKeys, const vector<termpayload_t>& termPayloads,
                                  const vector<vector<docid_t>>& expectDocIdLists)
    {
        string adaptiveDictFilePath = util::PathUtil::JoinPath(mRootDir, BITMAP_DICTIONARY_FILE_NAME);
        string adaptivePostingFilePath = util::PathUtil::JoinPath(mRootDir, BITMAP_POSTING_FILE_NAME);

        INDEXLIB_TEST_TRUE(file_system::FslibWrapper::IsExist(adaptiveDictFilePath).GetOrThrow());
        INDEXLIB_TEST_TRUE(file_system::FslibWrapper::IsExist(adaptivePostingFilePath).GetOrThrow());

        RESET_FILE_SYSTEM();
        std::shared_ptr<index::OnDiskBitmapIndexIterator> ptr(new OnDiskBitmapIndexIterator(GET_PARTITION_DIRECTORY()));
        ptr->Init();

        index::DictKeyInfo key(0);

        for (size_t i = 0; i < dictKeys.size(); i++) {
            BitmapPostingDecoder* decoder = dynamic_cast<BitmapPostingDecoder*>(ptr->Next(key));
            const TermMeta* termMeta = decoder->GetTermMeta();
            INDEXLIB_TEST_EQUAL((df_t)(expectDocIdLists[i].size()), termMeta->GetDocFreq());
            INDEXLIB_TEST_EQUAL(termPayloads[i], termMeta->GetPayload());
            INDEXLIB_TEST_EQUAL(dictKeys[i], key);
            docid_t docBuffer[100];
            uint32_t decodedNum = decoder->DecodeDocList(docBuffer, 100);
            INDEXLIB_TEST_EQUAL(decodedNum, expectDocIdLists[i].size());
            for (size_t j = 0; j < decodedNum; j++) {
                INDEXLIB_TEST_EQUAL(expectDocIdLists[i][j], docBuffer[j]);
            }
        }
    }

    void CreateIndexDataWriters(BufferedFileWriterPtr& postingFile, std::shared_ptr<DictionaryWriter>& dictWriter)
    {
        string postingFileName = util::PathUtil::JoinPath(mRootDir, BITMAP_POSTING_FILE_NAME);

        dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(&mSimplePool));
        dictWriter->Open(GET_PARTITION_DIRECTORY(), BITMAP_DICTIONARY_FILE_NAME);

        postingFile.reset(new BufferedFileWriter());
        EXPECT_EQ(FSEC_OK, postingFile->Open(postingFileName, postingFileName));
    }

    void DestroyIndexDataWriters(BufferedFileWriterPtr& postingFile, std::shared_ptr<DictionaryWriter>& dictWriter)
    {
        dictWriter->Close();
        dictWriter.reset();

        ASSERT_EQ(FSEC_OK, postingFile->Close());
        postingFile.reset();
    }

    void TestCaseForAddPosting()
    {
        BufferedFileWriterPtr postingFile;
        std::shared_ptr<DictionaryWriter> dictWriter;
        CreateIndexDataWriters(postingFile, dictWriter);
        AdaptiveBitmapIndexWriterPtr adaptiveWriter = CreateAdaptiveBitmapIndexWriter();
        adaptiveWriter->Init(dictWriter, postingFile);

        vector<index::DictKeyInfo> expectDictKeys;
        vector<termpayload_t> expectTermPayloads;
        vector<vector<docid_t>> expectDocIdLists;
        AddAdaptiveBitmapPosting(adaptiveWriter, "1#2#1,2", expectDictKeys, expectTermPayloads, expectDocIdLists);
        AddAdaptiveBitmapPosting(adaptiveWriter, "3#4#1,2,3", expectDictKeys, expectTermPayloads, expectDocIdLists);
        // add empty posting list, not in adaptive bitmap
        AddAdaptiveBitmapPosting(adaptiveWriter, "3#4#", expectDictKeys, expectTermPayloads, expectDocIdLists);
        AddAdaptiveBitmapPosting(adaptiveWriter, (index::DictKeyInfo::NULL_TERM.ToString() + "#5#1,3"), expectDictKeys,
                                 expectTermPayloads, expectDocIdLists);
        DestroyIndexDataWriters(postingFile, dictWriter);
        CheckAdaptiveBitmapIndex(expectDictKeys, expectTermPayloads, expectDocIdLists);
        CheckGetAdaptiveDictKeys(adaptiveWriter, expectDictKeys);
    }

    void TestCaseForCollectDocIds()
    {
        vector<docid_t> docList;
        docList.push_back(1);
        docList.push_back(2);
        std::shared_ptr<PostingIterator> postingIter(new PostingIteratorMocker(docList));
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
        config::IndexConfigPtr indexConfig(new config::SingleFieldIndexConfig("title", it_text));
        trigger->Init(indexConfig);
        ArchiveFolderPtr archiveFoler(new ArchiveFolder(false));
        archiveFoler->Open(GET_PARTITION_DIRECTORY()->GetIDirectory()).GetOrThrow();
        return AdaptiveBitmapIndexWriterPtr(new AdaptiveBitmapIndexWriter(trigger, indexConfig, archiveFoler));
    }

    // docString : termHashKey#termPayload#docid1,docid2,docid3...
    void AddAdaptiveBitmapPosting(const AdaptiveBitmapIndexWriterPtr& adaptiveWriter, const string& docInfoStr,
                                  vector<index::DictKeyInfo>& expectTermKeys, vector<termpayload_t>& expectTermPayloads,
                                  vector<vector<docid_t>>& expectDocLists)
    {
        vector<string> docInfos = StringUtil::split(docInfoStr, "#", false);
        assert(docInfos.size() == 3);
        index::DictKeyInfo dictKey;
        termpayload_t termPayload;
        vector<docid_t> docList;
        dictKey.FromString(docInfos[0]);
        StringUtil::fromString(docInfos[1], termPayload);
        StringUtil::fromString(docInfos[2], docList, ",");

        std::shared_ptr<PostingIterator> postingIter(new PostingIteratorMocker(docList));
        adaptiveWriter->AddPosting(dictKey, termPayload, postingIter);
        if (docList.size() > 0) {
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
}}} // namespace indexlib::index::legacy
