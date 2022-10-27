#include "indexlib/index/normal/inverted_index/test/normal_index_segment_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/test/normal_index_reader_helper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"
#include "indexlib/index/normal/inverted_index/test/mock_index_reader.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/misc/exception.h"


using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexSegmentReaderTest);

NormalIndexSegmentReaderTest::NormalIndexSegmentReaderTest()
{
}

NormalIndexSegmentReaderTest::~NormalIndexSegmentReaderTest()
{
}

void NormalIndexSegmentReaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mFileSystem = GET_FILE_SYSTEM();
    mInMemDir = GET_SEGMENT_DIRECTORY();
    mLocalDir = GET_PARTITION_DIRECTORY();
}

void NormalIndexSegmentReaderTest::CaseTearDown()
{
}

void NormalIndexSegmentReaderTest::TestOpen()
{
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    NormalIndexReaderHelper::PrepareSegmentFiles(mLocalDir->GetPath(),
            "bitwords", indexConfig);

    NormalIndexSegmentReader segmentReader;
    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);
    ASSERT_NO_THROW(segmentReader.Open(indexConfig, segmentData));
}

void NormalIndexSegmentReaderTest::TestOpenWithoutDictionary()
{
    string caseDir = mLocalDir->GetPath();
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    NormalIndexReaderHelper::PrepareSegmentFiles(caseDir, "bitwords", 
            indexConfig);

    string indexdir = FileSystemWrapper::JoinPath(caseDir, INDEX_DIR_NAME);
    string bitwordsDir = FileSystemWrapper::JoinPath(indexdir, "bitwords");
    string dictFilePath = FileSystemWrapper::JoinPath(bitwordsDir, 
            DICTIONARY_FILE_NAME);
    mFileSystem->RemoveFile(dictFilePath);

    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);

    NormalIndexSegmentReader noDictSegmentReader;
    ASSERT_THROW(noDictSegmentReader.Open(indexConfig, segmentData), IndexCollapsedException);
}

void NormalIndexSegmentReaderTest::TestOpenWithoutDictionaryAndPosting()
{
    string caseDir = mLocalDir->GetPath();
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    string indexdir = FileSystemWrapper::JoinPath(caseDir, INDEX_DIR_NAME);
    string bitwordsDir = FileSystemWrapper::JoinPath(indexdir, "bitwords");
    FileSystemWrapper::MkDir(bitwordsDir, true);

    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);

    NormalIndexSegmentReader noDictSegmentReader;
    ASSERT_NO_THROW(noDictSegmentReader.Open(indexConfig, segmentData));
}

void NormalIndexSegmentReaderTest::TestOpenWithoutIndexNameDir()
{
    string caseDir = mLocalDir->GetPath();
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    string indexdir = FileSystemWrapper::JoinPath(caseDir, INDEX_DIR_NAME);

    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);

    NormalIndexSegmentReader noDictSegmentReader;
    ASSERT_NO_THROW(noDictSegmentReader.Open(indexConfig, segmentData));

    FileSystemWrapper::MkDir(indexdir, true);
    ASSERT_NO_THROW(noDictSegmentReader.Open(indexConfig, segmentData));
}

void NormalIndexSegmentReaderTest::TestGetSegmentPosting()
{
    docid_t docId = 100;

    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_string));
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(NO_TERM_FREQUENCY);
    IndexFormatOption option;
    option.Init(indexConfig);
    DictInlineFormatter formatter(option.GetPostingFormatOption());
    formatter.SetDocId(docId);
    uint64_t compressedValue;
    ASSERT_TRUE(formatter.GetDictInlineValue(compressedValue));
    dictvalue_t dictValue = ShortListOptimizeUtil::CreateDictInlineValue(compressedValue);
 
    MockTieredDictionaryReaderPtr dictReader(new MockTieredDictionaryReader());
    EXPECT_CALL(*dictReader, Lookup(_, _))
        .WillRepeatedly(DoAll(SetArgReferee<1>(dictValue), Return(true)));

    NormalIndexSegmentReader segReader;
    segReader.mOption = option;
    segReader.mDictReader = dictReader;

    SegmentPosting segPosting;
    ASSERT_TRUE(segReader.GetSegmentPosting(0, 0, segPosting, NULL));

    ASSERT_EQ(ShortListOptimizeUtil::GetCompressMode(dictValue),
              segPosting.GetCompressMode());
    ASSERT_EQ(ShortListOptimizeUtil::GetDictInlineValue(dictValue),
              segPosting.GetDictInlinePostingData());
}

IE_NAMESPACE_END(index);

