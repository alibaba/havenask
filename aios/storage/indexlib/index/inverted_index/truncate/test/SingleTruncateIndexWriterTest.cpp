#include "indexlib/index/inverted_index/truncate/SingleTruncateIndexWriter.h"

#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/truncate/AttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/NoSortTruncateCollector.h"
#include "indexlib/index/inverted_index/truncate/test/FakePostingIterator.h"
#include "indexlib/index/inverted_index/truncate/test/MockHelper.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class SingleTruncateIndexWriterTest : public TESTBASE
{
public:
    void setUp() override { _root = GET_TEMP_DATA_PATH(); }
    void tearDown() override {}

private:
    std::string _root;
};

TEST_F(SingleTruncateIndexWriterTest, TestWriteTruncateMeta)
{
    // create single truncate index writer
    auto invertedIndexConfig = std::make_shared<indexlibv2::config::PackageIndexConfig>("nid", it_string);
    std::shared_ptr<indexlibv2::index::DocMapper> docMapper;
    auto truncateIndexWriter = std::make_shared<SingleTruncateIndexWriter>(invertedIndexConfig, docMapper);
    // set param

    static uint32_t EXPECTED_DOC_COUNT = 5;
    auto bucketVecAllocator = std::make_shared<BucketVectorAllocator>();
    auto docCollector = std::make_shared<NoSortTruncateCollector>(EXPECTED_DOC_COUNT, EXPECTED_DOC_COUNT, nullptr,
                                                                  nullptr, bucketVecAllocator);
    std::vector<docid_t> docIds;
    for (size_t i = 0; i < 10; ++i) {
        docIds.push_back((docid_t)i);
    }
    auto postingIt = std::make_shared<FakePostingIterator>(docIds);
    docCollector->CollectDocIds(index::DictKeyInfo(0), postingIt, -1);

    auto allocator = std::make_shared<DocInfoAllocator>();
    auto refer = allocator->DeclareReference(/*fieldName*/ "price", ft_int8, false);
    [[maybe_unused]] auto docInfo = allocator->Allocate();

    file_system::IOConfig ioConfig;

    auto truncateAttrReader = std::make_shared<MockTruncateAttributeReader>();
    int8_t val = 40;
    EXPECT_CALL(*truncateAttrReader, CreateReadContextPtr(_)).WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*truncateAttrReader, Read(_, _, _, _, _, _))
        .WillRepeatedly(DoAll(::testing::SetArgPointee<2>(val), ::testing::SetArgReferee<4>(false), Return(true)));
    auto attrEvaluator = std::make_shared<AttributeEvaluator<int8_t>>(truncateAttrReader, refer);

    truncateIndexWriter->SetParam(attrEvaluator, docCollector, nullptr, "", allocator, ioConfig);
    // set truncate index meta info
    auto truncateMetaDir = indexlib::file_system::Directory::GetPhysicalDirectory(_root);
    auto metaFileWriter =
        truncateMetaDir->CreateFileWriter("truncate", indexlib::file_system::WriterOption::AtomicDump());
    truncateIndexWriter->SetTruncateIndexMetaInfo(metaFileWriter, "price", false);

    // check
    std::string metaValue;
    truncateIndexWriter->GenerateMetaValue(postingIt, index::DictKeyInfo(2), metaValue);
    ASSERT_EQ(metaValue, "2\t40\n");

    truncateIndexWriter->GenerateMetaValue(postingIt, index::DictKeyInfo::NULL_TERM, metaValue);
    ASSERT_EQ(metaValue, "18446744073709551615:true\t40\n");
}

TEST_F(SingleTruncateIndexWriterTest, TestHasTruncatedAndSaveDictKey)
{
    auto meaninglessIndexConfig = std::make_shared<indexlibv2::config::PackageIndexConfig>("nid", it_string);
    auto writer = std::make_unique<SingleTruncateIndexWriter>(meaninglessIndexConfig, /*docMapper*/ nullptr);
    DictKeyInfo info(/*key*/ 1, /*isNull*/ true);
    ASSERT_FALSE(writer->HasTruncated(info));
    ASSERT_FALSE(writer->_hasNullTerm);

    writer->SaveDictKey(info);
    ASSERT_TRUE(writer->_hasNullTerm);
    ASSERT_EQ(0, writer->_truncateDictKeySet.size());

    DictKeyInfo info1(/*key*/ 2, /*isNull*/ false);
    writer->SaveDictKey(info1);
    ASSERT_TRUE(writer->_hasNullTerm);
    ASSERT_EQ(1, writer->_truncateDictKeySet.size());
    ASSERT_TRUE(writer->HasTruncated(info1));

    // save same key
    writer->SaveDictKey(info1);
    ASSERT_TRUE(writer->_hasNullTerm);
    ASSERT_EQ(1, writer->_truncateDictKeySet.size());
    ASSERT_TRUE(writer->HasTruncated(info1));
}

TEST_F(SingleTruncateIndexWriterTest, TestCreatePostingWriter)
{
    auto meaninglessIndexConfig = std::make_shared<indexlibv2::config::PackageIndexConfig>("nid", it_string);
    auto writer = std::make_unique<SingleTruncateIndexWriter>(meaninglessIndexConfig, /*docMapper*/ nullptr);
    ASSERT_TRUE(writer->CreatePostingWriter(it_primarykey64) == nullptr);
    ASSERT_TRUE(writer->CreatePostingWriter(it_primarykey128) == nullptr);
    ASSERT_TRUE(writer->CreatePostingWriter(it_trie) == nullptr);
    writer->_postingWriterResource = std::make_shared<PostingWriterResource>(
        /*simplePool*/ nullptr, /*slicePool*/ nullptr, /*bufferPool*/ nullptr, PostingFormatOption());
    ASSERT_TRUE(writer->CreatePostingWriter(it_string) != nullptr);
}
} // namespace indexlib::index
