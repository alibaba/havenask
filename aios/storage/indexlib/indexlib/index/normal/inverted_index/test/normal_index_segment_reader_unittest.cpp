#include "indexlib/index/normal/inverted_index/test/normal_index_segment_reader_unittest.h"

#include "indexlib/config/single_field_index_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/normal/inverted_index/test/mock_index_reader.h"
#include "indexlib/index/normal/inverted_index/test/normal_index_reader_helper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, NormalIndexSegmentReaderTest);

NormalIndexSegmentReaderTest::NormalIndexSegmentReaderTest() {}

NormalIndexSegmentReaderTest::~NormalIndexSegmentReaderTest() {}

void NormalIndexSegmentReaderTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mFileSystem = GET_FILE_SYSTEM();
    mInMemDir = GET_SEGMENT_DIRECTORY();
    mLocalDir = GET_PARTITION_DIRECTORY();
}

void NormalIndexSegmentReaderTest::CaseTearDown() {}

void NormalIndexSegmentReaderTest::TestOpen()
{
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    NormalIndexReaderHelper::PrepareSegmentFiles(mLocalDir, "bitwords", indexConfig);

    NormalIndexSegmentReader segmentReader;
    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);
    ASSERT_NO_THROW(segmentReader.Open(indexConfig, segmentData, nullptr));
}

void NormalIndexSegmentReaderTest::TestOpenWithoutDictionary()
{
    string caseDir = mRootDir;
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    NormalIndexReaderHelper::PrepareSegmentFiles(mLocalDir, "bitwords", indexConfig);

    mLocalDir->RemoveFile(util::PathUtil::JoinPath(INDEX_DIR_NAME, "bitwords", DICTIONARY_FILE_NAME));

    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);

    NormalIndexSegmentReader noDictSegmentReader;
    ASSERT_THROW(noDictSegmentReader.Open(indexConfig, segmentData, nullptr), IndexCollapsedException);
}

void NormalIndexSegmentReaderTest::TestOpenWithoutDictionaryAndPosting()
{
    string caseDir = mRootDir;
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    string indexdir = util::PathUtil::JoinPath(caseDir, INDEX_DIR_NAME);
    string bitwordsDir = util::PathUtil::JoinPath(indexdir, "bitwords");
    FslibWrapper::MkDirE(bitwordsDir, true);

    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);

    NormalIndexSegmentReader noDictSegmentReader;
    ASSERT_NO_THROW(noDictSegmentReader.Open(indexConfig, segmentData, nullptr));
}

void NormalIndexSegmentReaderTest::TestOpenWithoutIndexNameDir()
{
    string caseDir = mRootDir;
    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_text));
    string indexdir = util::PathUtil::JoinPath(caseDir, INDEX_DIR_NAME);

    index_base::SegmentData segmentData;
    segmentData.SetDirectory(mLocalDir);

    NormalIndexSegmentReader noDictSegmentReader;
    ASSERT_NO_THROW(noDictSegmentReader.Open(indexConfig, segmentData, nullptr));

    FslibWrapper::MkDirE(indexdir, true);
    ASSERT_NO_THROW(noDictSegmentReader.Open(indexConfig, segmentData, nullptr));
}

void NormalIndexSegmentReaderTest::TestGetSegmentPosting()
{
    docid_t docId = 100;

    IndexConfigPtr indexConfig(new SingleFieldIndexConfig("bitwords", it_string));
    indexConfig->SetOptionFlag(NO_TERM_FREQUENCY);
    LegacyIndexFormatOption option;
    option.Init(indexConfig);
    DictInlineFormatter formatter(option.GetPostingFormatOption());
    formatter.SetDocId(docId);
    uint64_t compressedValue;
    ASSERT_TRUE(formatter.GetDictInlineValue(compressedValue));
    dictvalue_t dictValue = ShortListOptimizeUtil::CreateDictInlineValue(compressedValue, false, true);

    MockTieredDictionaryReaderPtr dictReader(new MockTieredDictionaryReader());
    EXPECT_CALL(*dictReader, DoInnerLookup(_, _)).WillRepeatedly(DoAll(SetArgReferee<1>(dictValue), Return(true)));

    NormalIndexSegmentReader segReader;
    segReader.mOption = option;
    segReader.mDictReader = dictReader;

    SegmentPosting segPosting;
    ASSERT_TRUE(segReader.GetSegmentPosting(index::DictKeyInfo(0), 0, segPosting, NULL));

    ASSERT_EQ(ShortListOptimizeUtil::GetCompressMode(dictValue), segPosting.GetCompressMode());
    ASSERT_EQ(ShortListOptimizeUtil::GetDictInlineValue(dictValue), segPosting.GetDictInlinePostingData());
}
}} // namespace indexlib::index
