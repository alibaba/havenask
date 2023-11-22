#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/mock/FakeDiskSegment.h"
#include "indexlib/framework/mock/FakeMemSegment.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/primary_key/BlockPrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/HashPrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriterCreator.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/primary_key/SortedPrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/test/PrimaryKeyReaderTesthelper.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class PrimaryKeyWriterTest : public TESTBASE
{
public:
    PrimaryKeyWriterTest() {}
    ~PrimaryKeyWriterTest() {}

public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename Key>
    void TestInternal(PrimaryKeyIndexType indexType);

    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
    CreateIndexConfig(uint64_t key, const std::string& indexName, PrimaryKeyIndexType indexType);
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
    CreateIndexConfig(autil::uint128_t key, const std::string& indexName, PrimaryKeyIndexType indexType);

    template <typename Key>
    void MakeOneSegment(segmentid_t segmentId, const std::string& pksStr);

    template <typename Key>
    void CreateData(const std::string& pksStr);

    template <typename Key>
    shared_ptr<framework::TabletData> CreateTabletData(const vector<int>& segIds, bool needMemSegment = false);
    template <typename Key>
    shared_ptr<IIndexer> CreateDiskIndexer(const shared_ptr<indexlib::file_system::Directory>& directory,
                                           uint32_t docCount);

private:
    std::string _rootPath;
    indexlib::file_system::IFileSystemPtr _fs;
    indexlib::file_system::DirectoryPtr _rootDir;
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> _indexConfig;
    indexlib::util::SimplePool _pool;
};

void PrimaryKeyWriterTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    _rootPath = GET_TEMP_DATA_PATH();
    _fs = indexlib::file_system::FileSystemCreator::Create("test", _rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(_fs);
}

void PrimaryKeyWriterTest::tearDown() {}

template <typename Key>
void PrimaryKeyWriterTest::CreateData(const std::string& pksStr)
{
    autil::StringTokenizer st(pksStr, ";",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    segmentid_t segmentId = 0;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        MakeOneSegment<Key>(segmentId, st[i]);
        segmentId++;
    }
}
template <typename Key>
shared_ptr<IIndexer>
PrimaryKeyWriterTest::CreateDiskIndexer(const shared_ptr<indexlib::file_system::Directory>& directory,
                                        uint32_t docCount)
{
    DiskIndexerParameter parameter {.docCount = docCount};
    auto diskIndexer = make_shared<PrimaryKeyDiskIndexer<Key>>(parameter);
    auto status = diskIndexer->Open(_indexConfig, directory->GetIDirectory());
    assert(status.IsOK());
    return diskIndexer;
}

template <typename Key>
shared_ptr<framework::TabletData> PrimaryKeyWriterTest::CreateTabletData(const vector<int>& segIds, bool needMemSegment)
{
    auto tabletData = make_shared<framework::TabletData>("pktest");
    std::shared_ptr<framework::ResourceMap> map(new framework::ResourceMap());
    framework::BuildResource buildResource;
    vector<shared_ptr<framework::Segment>> segments;
    segments.reserve(segIds.size());
    for (auto segmentId : segIds) {
        std::string segDirName = "segment_" + to_string(segmentId) + "_level_0";
        auto segInfo = std::make_shared<indexlibv2::framework::SegmentInfo>();
        auto segDir = _rootDir->GetDirectory(segDirName, false);
        assert(segDir);
        auto status = segInfo->Load(segDir->GetIDirectory(),
                                    indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM));
        assert(status.IsOK());

        framework::SegmentMeta segMeta(segmentId);
        segMeta.segmentInfo = segInfo;
        segMeta.segmentDir = segDir;
        auto segment = std::make_shared<framework::FakeDiskSegment>(segMeta);
        auto pkDir = segDir->GetDirectory("index/", false);
        assert(pkDir);
        auto diskIndexer = CreateDiskIndexer<Key>(pkDir, segInfo->docCount);
        assert(diskIndexer);
        segment->AddIndexer(_indexConfig->GetIndexType(), _indexConfig->GetIndexName(), diskIndexer);
        segments.push_back(segment);
    }

    if (needMemSegment) {
        framework::SegmentMeta segMeta(110120);
        auto segment = std::make_shared<framework::FakeMemSegment>(segMeta);
        auto currentBuildDocId = std::make_shared<const docid64_t>(0);
        auto memIndexer = std::make_shared<PrimaryKeyWriter<Key>>(nullptr, currentBuildDocId);
        if (!memIndexer->Init(_indexConfig, nullptr).IsOK()) {
            assert(false);
            return nullptr;
        }
        Key primaryKey;
        std::string field = "memIndexer";
        indexlib::index::KeyHasherWrapper::GetHashKey(ft_string, pk_default_hash, field.data(), field.size(),
                                                      primaryKey);
        auto hashMap = (indexlib::util::HashMap<Key, docid_t>*)memIndexer->GetHashMap().get();
        hashMap->FindAndInsert(primaryKey, 0);
        segment->AddIndexer(_indexConfig->GetIndexType(), _indexConfig->GetIndexName(), memIndexer);
        segments.push_back(segment);
    }
    auto status = tabletData->Init(/*invalid version*/ framework::Version(), segments, map);
    assert(status.IsOK());
    return tabletData;
}

template <typename Key>
void PrimaryKeyWriterTest::TestInternal(PrimaryKeyIndexType indexType)
{
    _indexConfig = CreateIndexConfig((Key)0, "pk", indexType);
    ASSERT_TRUE(_indexConfig->GetInvertedIndexType() == it_primarykey64 ||
                _indexConfig->GetInvertedIndexType() == it_primarykey128);

    // Prepare build data
    std::string pksStr("pkstr0:0,pkstr1:1,pkstr2:2;"
                       "pkstr3:0,pkstr4:1,pkstr5:2;pkstr6:0,pkstr7:1,pkstr8:2");
    CreateData<Key>(pksStr);

    std::vector<int> segments {0, 1, 2};
    auto tabletData = CreateTabletData<Key>(segments, true);
    ASSERT_TRUE(tabletData);
    PrimaryKeyReader<Key> reader(nullptr);
    ASSERT_TRUE(reader.Open(_indexConfig, tabletData.get()).IsOK());
    docid_t answer[] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    uint32_t answerSize = sizeof(answer) / sizeof(answer[0]);
    for (uint32_t i = 0; i < answerSize; ++i) {
        stringstream ss;
        ss << "pkstr" << i;
        ASSERT_EQ(answer[i], reader.Lookup(ss.str(), nullptr));
    }
}

std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
PrimaryKeyWriterTest::CreateIndexConfig(uint64_t key, const std::string& indexName, PrimaryKeyIndexType indexType)
{
    return PrimaryKeyReaderTesthelper::CreatePrimaryKeyIndexConfig<uint64_t>("pk", indexType, /*hasPKAttribute*/ false);
}

std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
PrimaryKeyWriterTest::CreateIndexConfig(autil::uint128_t key, const std::string& indexName,
                                        PrimaryKeyIndexType indexType)
{
    return PrimaryKeyReaderTesthelper::CreatePrimaryKeyIndexConfig<autil::uint128_t>("pk", indexType,
                                                                                     /*hasPKAttribute*/ false);
}

template <typename Key>
void PrimaryKeyWriterTest::MakeOneSegment(segmentid_t segmentId, const std::string& pksStr)
{
    indexlib::file_system::DirectoryPtr segmentDirectory =
        _rootDir->MakeDirectory(std::string("segment_") + autil::StringUtil::toString(segmentId) + "_level_0",
                                indexlib::file_system::DirectoryOption::Mem());
    indexlib::file_system::DirectoryPtr indexDirectory = segmentDirectory->MakeDirectory(INDEX_DIR_NAME);

    const auto& indexName = _indexConfig->GetIndexName();
    Status status;
    std::shared_ptr<indexlib::file_system::IDirectory> pkDirectory;
    std::tie(status, pkDirectory) = indexDirectory->GetIDirectory()
                                        ->MakeDirectory(indexName, indexlib::file_system::DirectoryOption())
                                        .StatusWith();
    assert(status.IsOK());

    auto pkConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(_indexConfig);
    ASSERT_TRUE(pkConfig);
    // Create pk file writer
    auto fileWriter =
        pkDirectory->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME, indexlib::file_system::WriterOption()).Value();
    auto primaryKeyFileWriter = PrimaryKeyFileWriterCreator<Key>::CreatePKFileWriter(pkConfig);
    autil::StringTokenizer st(pksStr, ",",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    auto docCount = st.getNumTokens();
    primaryKeyFileWriter->Init(docCount, docCount, fileWriter, &_pool);

    Key primaryKey;
    auto fieldConfig = pkConfig->GetFieldConfig();
    auto fieldType = fieldConfig->GetFieldType();
    for (size_t i = 0; i < docCount; ++i) {
        autil::StringTokenizer st2(st[i], ":",
                                   autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
        assert(st2.getNumTokens() == 2);
        const auto& keyStr = st2[0];
        [[maybe_unused]] bool ret = indexlib::index::KeyHasherWrapper::GetHashKey(
            fieldType, pkConfig->GetPrimaryKeyHashType(), keyStr.c_str(), keyStr.size(), primaryKey);
        assert(ret);
        docid_t docId;
        autil::StringUtil::strToUInt32(st2[1].c_str(), (uint32_t&)docId);

        if (pkConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
            status = primaryKeyFileWriter->AddPKPair((Key)primaryKey, docId);
            assert(status.IsOK());
        } else {
            status = primaryKeyFileWriter->AddPKPair((Key)primaryKey, docId);
            // TODO(tianwei): make sure primaryKey is sorted.
            // status = primaryKeyFileWriter->AddSortedPKPair((Key)primaryKey, docId);
            assert(status.IsOK());
        }
    }

    status = primaryKeyFileWriter->Close();
    assert(status.IsOK());
    framework::SegmentInfo segInfo;
    segInfo.docCount = docCount;
    status = segInfo.Store(segmentDirectory->GetIDirectory());
    assert(status.IsOK());
    _rootDir->Sync(true);
}

TEST_F(PrimaryKeyWriterTest, TestHashPrimaryKeyWriterUint64) { TestInternal<uint64_t>(pk_hash_table); }
TEST_F(PrimaryKeyWriterTest, TestHashPrimaryKeyWriterUint128) { TestInternal<autil::uint128_t>(pk_hash_table); }

TEST_F(PrimaryKeyWriterTest, TestBlockPrimaryKeyWriterUint64) { TestInternal<uint64_t>(pk_block_array); }
TEST_F(PrimaryKeyWriterTest, TestBlockPrimaryKeyWriterUint128) { TestInternal<autil::uint128_t>(pk_block_array); }

TEST_F(PrimaryKeyWriterTest, TestSortedPrimaryKeyWriterUint64) { TestInternal<uint64_t>(pk_sort_array); }
TEST_F(PrimaryKeyWriterTest, TestSortedPrimaryKeyWriterUint128) { TestInternal<autil::uint128_t>(pk_sort_array); }

TEST_F(PrimaryKeyWriterTest, TestDocid64)
{
    using Key = uint64_t;
    _indexConfig = CreateIndexConfig((Key)0, "pk", pk_hash_table);
    ASSERT_TRUE(_indexConfig->GetInvertedIndexType() == it_primarykey64 ||
                _indexConfig->GetInvertedIndexType() == it_primarykey128);

    // Prepare build data
    std::string pksStr("pkstr0:0,pkstr1:1,pkstr2:2,pkstr3:3,pkstr4:4;");
    CreateData<Key>(pksStr);

    std::vector<int> segments {0};
    auto tabletData = CreateTabletData<Key>(segments, true);
    ASSERT_TRUE(tabletData);
    PrimaryKeyReader<Key> reader(nullptr);
    ASSERT_TRUE(reader.Open(_indexConfig, tabletData.get()).IsOK());

    ASSERT_FALSE(reader._segmentReaderList.empty());
    ASSERT_FALSE(reader._buildingIndexReader->mInnerSegReaderItems.empty());

    reader._segmentReaderList[0]._segmentPair.first = std::numeric_limits<docid_t>::max() - 1;
    get<0>(reader._buildingIndexReader->mInnerSegReaderItems[0]) = 2147483651L;
    ASSERT_EQ(2147483650L, reader.Lookup("pkstr4", nullptr));
    ASSERT_EQ(2147483651L, reader.Lookup("memIndexer", nullptr));
}

} // namespace indexlibv2::index
