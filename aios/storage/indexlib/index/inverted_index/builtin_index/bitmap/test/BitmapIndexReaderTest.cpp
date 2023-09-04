#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexReader.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/test/BitmapPostingMaker.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::config;
using indexlibv2::framework::Version;

namespace indexlibv2 { namespace index {
class MockBitmapIndexReader : public BitmapIndexReader
{
public:
    MockBitmapIndexReader()
    {
        shared_ptr<config::SingleFieldIndexConfig> indexConfig(
            new config::SingleFieldIndexConfig("indexname", it_string));
        _indexConfig = indexConfig;
    }
    MOCK_METHOD(BitmapPostingIterator*, CreateBitmapPostingIterator,
                (autil::mem_pool::Pool * sessionPool,
                 indexlib::util::PooledUniquePtr<indexlib::index::InvertedIndexSearchTracer>),
                (const, override));
};

class MockInMemBitmapIndexSegmentReader : public InMemBitmapIndexSegmentReader
{
public:
    MockInMemBitmapIndexSegmentReader() : InMemBitmapIndexSegmentReader(NULL, false) {}

    MOCK_METHOD(bool, GetSegmentPosting,
                (const indexlib::index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                 autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption,
                 indexlib::index::InvertedIndexSearchTracer*),
                (const, override));
};

class MockBitmapPostingIterator : public BitmapPostingIterator
{
public:
    bool Init(const shared_ptr<SegmentPostingVector>& segPostings, const uint32_t statePoolSize) override
    {
        _segPostings = segPostings;
        return true;
    }

    shared_ptr<SegmentPostingVector> _segPostings;
};

class BitmapIndexReaderTest : public TESTBASE
{
public:
    using Key2DocIdListMap = BitmapPostingMaker::Key2DocIdListMap;
    void setUp() override
    {
        string caseDir = GET_TEMP_DATA_PATH();
        FslibWrapper::DeleteDirE(caseDir, DeleteOption::NoFence(true));
        FslibWrapper::MkDirE(caseDir);

        FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = true;
        shared_ptr<indexlib::util::MemoryQuotaController> mqc(
            new indexlib::util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        shared_ptr<indexlib::util::PartitionMemoryQuotaController> quotaController(
            new indexlib::util::PartitionMemoryQuotaController(mqc));
        fileSystemOptions.memoryQuotaController = quotaController;
        _fileSystem = FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                /*rootPath=*/caseDir, fileSystemOptions,
                                                shared_ptr<indexlib::util::MetricProvider>(),
                                                /*isOverride=*/false)
                          .GetOrThrow();

        DirectoryPtr directory = IDirectory::ToLegacyDirectory(make_shared<MemDirectory>("", _fileSystem));
        _testDir = directory->MakeDirectory("test");
        _fileSystem->Sync(true).GetOrThrow();
    }

    indexlibv2::framework::Version MakeVersion(versionid_t id, const std::string& segments)
    {
        indexlibv2::framework::Version version(id);
        auto vec = autil::StringUtil::split(segments, ',');
        for (auto i : vec) {
            segmentid_t segId = 0;
            [[maybe_unused]] auto ret = autil::StringUtil::fromString(i, segId);
            assert(ret);
            version.AddSegment(segId);
        }
        return version;
    }

    shared_ptr<BitmapIndexReader> OpenBitmapIndex(const Version& version, const string& fieldName)
    {
        auto schema = table::NormalTabletSchemaMaker::Make(
            "bitmap:string;non_updatable_bitmap:text",
            "bitmap:string:bitmap;non_updatable_bitmap:text:non_updatable_bitmap", "", "");
        auto indexConfig = dynamic_pointer_cast<config::InvertedIndexConfig>(
            schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, fieldName));
        assert(indexConfig);
        if (fieldName == "bitmap") {
            indexConfig->TEST_SetIndexUpdatable(true);
        }
        auto vocabulary = make_shared<HighFrequencyVocabulary>();
        indexConfig->SetHighFreqVocabulary(vocabulary);
        vector<pair<docid_t, shared_ptr<BitmapLeafReader>>> diskReaders;
        auto status = GetBitmapDiskReaders(indexConfig, version, diskReaders);
        if (!status.IsOK()) {
            return nullptr;
        }
        shared_ptr<BitmapIndexReader> reader(new BitmapIndexReader);
        status = reader->Open(indexConfig, diskReaders, {});
        assert(status.IsOK());
        return reader;
    }

    Status GetBitmapDiskReaders(const shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                const Version& version,
                                vector<pair<docid_t, shared_ptr<BitmapLeafReader>>>& diskSegmentReaders)
    {
        docid_t baseDocId = 0;
        for (auto [segId, schemaId] : version) {
            string segDirName = version.GetSegmentDirName(segId);
            framework::SegmentInfo segInfo;
            auto status =
                segInfo.Load(_testDir->GetIDirectory()->GetDirectory(segDirName).GetOrThrow(), ReaderOption(FSOT_MEM));
            if (!status.IsOK()) {
                return status;
            }
            auto docCount = segInfo.docCount;
            if (docCount == 0) {
                continue;
            }
            auto indexer = make_shared<BitmapDiskIndexer>(docCount);
            auto indexDir = _testDir->GetDirectory(segDirName + "/index", false);
            assert(indexDir);
            status = indexer->Open(indexConfig, indexDir->GetIDirectory());
            if (!status.IsOK()) {
                return status;
            }
            diskSegmentReaders.emplace_back(baseDocId, indexer->GetReader());
            baseDocId += docCount;
        }
        return Status::OK();
    }

    void CheckData(const shared_ptr<BitmapIndexReader>& reader, Key2DocIdListMap& answer)
    {
        Key2DocIdListMap::const_iterator it;
        for (it = answer.begin(); it != answer.end(); ++it) {
            const string& key = it->first;
            CheckOnePosting(reader, key, it->second);
        }
    }
    void CheckOnePosting(const shared_ptr<BitmapIndexReader>& reader, const string& key,
                         const vector<docid_t>& docIdList)
    {
        indexlib::index::Term term("bitmap");
        if (key != "__NULL__") {
            term.SetWord(key);
        }
        shared_ptr<PostingIterator> postIt(reader->Lookup(term, 1000, nullptr, nullptr).ValueOrThrow());
        ASSERT_TRUE(postIt);

        for (uint32_t i = 0; i < docIdList.size(); ++i) {
            docid_t id = docIdList[i];
            docid_t seekedId = postIt->SeekDoc(id);
            ASSERT_EQ(seekedId, id);
        }
    }

    void WriteBadDataToFile(const string& fileName)
    {
        auto writer = _testDir->CreateFileWriter(fileName);
        uint8_t badContent[64] = {0xff};
        ASSERT_TRUE(writer->Write(badContent, sizeof(badContent)).OK());
        ASSERT_TRUE(writer->Close().OK());
    }

private:
    shared_ptr<Directory> _testDir;
    shared_ptr<IFileSystem> _fileSystem;
};

TEST_F(BitmapIndexReaderTest, testCaseForOneSegment)
{
    Key2DocIdListMap answer;
    segmentid_t segId = 0;
    uint32_t docNum = 1;
    docid_t baseDocId = 0;
    BitmapPostingMaker maker(_testDir, "bitmap", /*enableNullTerm=*/true);
    maker.MakeOneSegmentData(segId, docNum, baseDocId, &answer);
    Version version = MakeVersion(0, "0");

    shared_ptr<BitmapIndexReader> indexReader = OpenBitmapIndex(version, /*fieldName=*/"bitmap");
    ASSERT_TRUE(indexReader);
    CheckData(indexReader, answer);
}

TEST_F(BitmapIndexReaderTest, testCaseForMultiSegments)
{
    Key2DocIdListMap answer;
    uint32_t docNums[] = {12, 5, 0, 7, 0, 16};
    const uint32_t SEG_NUM = sizeof(docNums) / sizeof(docNums[0]);

    vector<uint32_t> docNumVect(docNums, docNums + SEG_NUM);
    BitmapPostingMaker maker(_testDir, "bitmap", /*enableNullTerm=*/true);
    maker.MakeMultiSegmentsData(docNumVect, &answer);
    Version version = MakeVersion(0, "0,1,2,3,4,5");

    shared_ptr<BitmapIndexReader> indexReader = OpenBitmapIndex(version, /*fieldName=*/"bitmap");
    ASSERT_TRUE(indexReader);
    CheckData(indexReader, answer);
}

TEST_F(BitmapIndexReaderTest, testCaseForBadDictionary)
{
    Key2DocIdListMap answer;
    BitmapPostingMaker maker(_testDir, "bitmap", /*enableNullTerm=*/true);
    maker.MakeOneSegmentData(0, 10, 0, &answer);
    Version version = MakeVersion(0, "0");

    // write error dictionary
    stringstream ss;
    ss << version.GetSegmentDirName(0) << "/" << INDEX_DIR_NAME << "/"
       << "bitmap/";
    string dictFileName = ss.str() + BITMAP_DICTIONARY_FILE_NAME;
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    _testDir->RemoveFile(dictFileName, removeOption /* may not exist */);
    WriteBadDataToFile(dictFileName);

    // open bitmap index and expect exception.
    ASSERT_FALSE(OpenBitmapIndex(version, /*fieldName=*/"bitmap"));
}

TEST_F(BitmapIndexReaderTest, TestCaseForBadPosting)
{
    Key2DocIdListMap answer;
    BitmapPostingMaker maker(_testDir, "bitmap", /*enableNullTerm=*/true);
    maker.MakeOneSegmentData(0, 10, 0, &answer);

    Version version = MakeVersion(0, "0");

    // remove posting file
    stringstream ss;
    ss << version.GetSegmentDirName(0) << "/" << INDEX_DIR_NAME << "/"
       << "bitmap/";
    string dataFileName = ss.str() + BITMAP_POSTING_FILE_NAME;
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    _testDir->RemoveFile(dataFileName, removeOption);

    // open bitmap index and expect failed.
    ASSERT_FALSE(OpenBitmapIndex(version, /*fieldName=*/"bitmap"));
    // ASSERT_THROW(OpenBitmapIndex(version, /*fieldName=*/"bitmap"), indexlib::util::NonExistException);
}

TEST_F(BitmapIndexReaderTest, testCaseForLookupWithRealtimeSegment)
{
    MockBitmapIndexReader bitmapReader;

    MockInMemBitmapIndexSegmentReader* mockSegmentReader = new MockInMemBitmapIndexSegmentReader();
    shared_ptr<InMemBitmapIndexSegmentReader> bitmapSegmentReader(mockSegmentReader);
    bitmapReader.AddInMemSegmentReader(0, bitmapSegmentReader);

    EXPECT_CALL(*mockSegmentReader, GetSegmentPosting(_, 0, _, _, _, _)).WillOnce(Return(false)).WillOnce(Return(true));

    MockBitmapPostingIterator bitmapIter;
    EXPECT_CALL(bitmapReader, CreateBitmapPostingIterator(_, _)).WillRepeatedly(Return(&bitmapIter));

    PostingIterator* postingIter =
        bitmapReader.Lookup(indexlib::index::DictKeyInfo(0), {}, 0, NULL, nullptr).ValueOrThrow();
    ASSERT_EQ(&bitmapIter, postingIter);
    ASSERT_EQ((size_t)0, bitmapIter._segPostings->size());
    postingIter = bitmapReader.Lookup(indexlib::index::DictKeyInfo(0), {}, 0, NULL, nullptr).ValueOrThrow();
    ASSERT_EQ(&bitmapIter, postingIter);
    ASSERT_EQ((size_t)1, bitmapIter._segPostings->size());
}

}} // namespace indexlibv2::index
