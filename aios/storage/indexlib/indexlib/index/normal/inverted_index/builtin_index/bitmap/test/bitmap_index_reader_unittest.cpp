#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_index_reader_unittest.h"

#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {

MockBitmapIndexReader::MockBitmapIndexReader()
{
    config::SingleFieldIndexConfigPtr indexConfig(new config::SingleFieldIndexConfig("indexname", it_string));
    SetIndexConfig(indexConfig);
}

void BitmapIndexReaderTest::CaseSetUp() { mTestDir = GET_TEMP_DATA_PATH(); }

void BitmapIndexReaderTest::CaseTearDown() {}

void BitmapIndexReaderTest::TestCaseForOneSegment()
{
    Key2DocIdListMap answer;
    segmentid_t segId = 0;
    uint32_t docNum = 1;
    docid_t baseDocId = 0;
    BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), "bitmap", /*enableNullTerm=*/true);
    maker.MakeOneSegmentData(segId, docNum, baseDocId, &answer);
    Version version = VersionMaker::Make(0, "0");
    version.Store(GET_PARTITION_DIRECTORY(), false);

    BitmapIndexReaderPtr indexReader = OpenBitmapIndex(mTestDir, /*fieldName=*/"bitmap");
    ASSERT_TRUE(indexReader);
    CheckData(indexReader, answer);
}

void BitmapIndexReaderTest::TestCaseForMultiSegments()
{
    Key2DocIdListMap answer;
    uint32_t docNums[] = {12, 5, 0, 7, 0, 16};
    const uint32_t SEG_NUM = sizeof(docNums) / sizeof(docNums[0]);

    vector<uint32_t> docNumVect(docNums, docNums + SEG_NUM);
    BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), "bitmap", /*enableNullTerm=*/true);
    maker.MakeMultiSegmentsData(docNumVect, &answer);
    Version version = VersionMaker::Make(0, "0,1,2,3,4,5");
    version.Store(GET_PARTITION_DIRECTORY(), false);

    BitmapIndexReaderPtr indexReader = OpenBitmapIndex(mTestDir, /*fieldName=*/"bitmap");
    ASSERT_TRUE(indexReader);
    CheckData(indexReader, answer);
}

void BitmapIndexReaderTest::TestCaseForBadDictionary()
{
    Key2DocIdListMap answer;
    BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), "bitmap", /*enableNullTerm=*/true);
    maker.MakeOneSegmentData(0, 10, 0, &answer);
    Version version = VersionMaker::Make(0, "0");
    version.Store(GET_PARTITION_DIRECTORY(), false);

    // write error dictionary
    stringstream ss;
    ss << version.GetSegmentDirName(0) << "/" << INDEX_DIR_NAME << "/"
       << "bitmap/";
    string dictFileName = ss.str() + BITMAP_DICTIONARY_FILE_NAME;
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    GET_PARTITION_DIRECTORY()->RemoveFile(dictFileName, removeOption /* may not exist */);
    WriteBadDataToFile(GET_PARTITION_DIRECTORY(), dictFileName);

    // open bitmap index and expect exception.
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("bitmap:string", "bitmap:string:bitmap", "", "");
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("bitmap");

    index_base::PartitionDataPtr partitionData =
        test::PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM(), "");
    BitmapIndexReaderPtr reader(new BitmapIndexReader);
    ASSERT_THROW(reader->Open(indexConfig, partitionData), FileIOException);
}

void BitmapIndexReaderTest::TestCaseForBadPosting()
{
    Key2DocIdListMap answer;
    BitmapPostingMaker maker(GET_PARTITION_DIRECTORY(), "bitmap", /*enableNullTerm=*/true);
    maker.MakeOneSegmentData(0, 10, 0, &answer);

    Version version = VersionMaker::Make(0, "0");
    version.Store(GET_PARTITION_DIRECTORY(), false);

    // remove posting file
    stringstream ss;
    ss << mTestDir << version.GetSegmentDirName(0) << "/" << INDEX_DIR_NAME << "/"
       << "bitmap/";
    string dataFileName = ss.str() + BITMAP_POSTING_FILE_NAME;
    fs::FileSystem::remove(dataFileName);

    // open bitmap index and expect failed.
    BitmapIndexReaderPtr reader(new BitmapIndexReader);

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("bitmap:string", "bitmap:string:bitmap", "", "");
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("bitmap");

    index_base::PartitionDataPtr partitionData =
        test::PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM(), "");
    ASSERT_THROW(reader->Open(indexConfig, partitionData), NonExistException);
}

void BitmapIndexReaderTest::TestCaseForLookupWithRealtimeSegment()
{
    MockBitmapIndexReader bitmapReader;

    MockInMemBitmapIndexSegmentReader* mockSegmentReader = new MockInMemBitmapIndexSegmentReader();
    InMemBitmapIndexSegmentReaderPtr bitmapSegmentReader(mockSegmentReader);
    bitmapReader.AddInMemSegmentReader(0, bitmapSegmentReader);

    EXPECT_CALL(*mockSegmentReader, GetSegmentPosting(_, 0, _, _, _, _)).WillOnce(Return(false)).WillOnce(Return(true));

    MockBitmapPostingIterator bitmapIter;
    EXPECT_CALL(bitmapReader, CreateBitmapPostingIterator(_)).WillRepeatedly(Return(&bitmapIter));

    PostingIterator* postingIter = bitmapReader.Lookup(index::DictKeyInfo(0), {}, 0, NULL, nullptr).ValueOrThrow();
    ASSERT_EQ(&bitmapIter, postingIter);
    ASSERT_EQ((size_t)0, bitmapIter.mSegPostings->size());
    postingIter = bitmapReader.Lookup(index::DictKeyInfo(0), {}, 0, NULL, nullptr).ValueOrThrow();
    ASSERT_EQ(&bitmapIter, postingIter);
    ASSERT_EQ((size_t)1, bitmapIter.mSegPostings->size());
}

void BitmapIndexReaderTest::CheckData(const BitmapIndexReaderPtr& reader, Key2DocIdListMap& answer)
{
    Key2DocIdListMap::const_iterator it;
    for (it = answer.begin(); it != answer.end(); ++it) {
        const string& key = it->first;
        CheckOnePosting(reader, key, it->second);
    }
}

void BitmapIndexReaderTest::CheckOnePosting(const BitmapIndexReaderPtr& reader, const string& key,
                                            const vector<docid_t>& docIdList)
{
    Term term("bitmap");
    if (key != "__NULL__") {
        term.SetWord(key);
    }
    PostingIteratorPtr postIt(reader->Lookup(term, 1000, nullptr, nullptr).ValueOrThrow());
    INDEXLIB_TEST_TRUE(postIt != NULL);

    for (uint32_t i = 0; i < docIdList.size(); ++i) {
        docid_t id = docIdList[i];
        docid_t seekedId = postIt->SeekDoc(id);
        INDEXLIB_TEST_EQUAL(seekedId, id);
    }
}

BitmapIndexReaderPtr BitmapIndexReaderTest::OpenBitmapIndex(const string& dir, const string& fieldName)
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("bitmap:string;non_updatable_bitmap:text",
                                "bitmap:string:bitmap;non_updatable_bitmap:text:non_updatable_bitmap", "", "");
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig(fieldName);
    if (fieldName == "bitmap") {
        indexConfig->TEST_SetIndexUpdatable(true);
    }

    index_base::PartitionDataPtr partitionData =
        test::PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM(), "");

    BitmapIndexReaderPtr reader(new BitmapIndexReader);
    bool ret = reader->Open(indexConfig, partitionData);
    if (ret) {
        return reader;
    }
    return BitmapIndexReaderPtr();
}

void BitmapIndexReaderTest::WriteBadDataToFile(const file_system::DirectoryPtr& rootDir, const string& fileName)
{
    auto writer = rootDir->CreateFileWriter(fileName);
    uint8_t badContent[64] = {0xff};
    writer->Write(badContent, sizeof(badContent)).GetOrThrow();
    ASSERT_EQ(file_system::FSEC_OK, writer->Close());
}
}}} // namespace indexlib::index::legacy
