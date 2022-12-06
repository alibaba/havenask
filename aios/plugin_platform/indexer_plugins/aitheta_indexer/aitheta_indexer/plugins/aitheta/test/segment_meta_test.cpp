#include "aitheta_indexer/plugins/aitheta/segment.h"
#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;

IE_NAMESPACE_BEGIN(aitheta_plugin);

class SegmentMetaTest : public AithetaTestBase {
 public:
    SegmentMetaTest() = default;
    ~SegmentMetaTest() = default;

 public:
    DECLARE_CLASS_NAME(SegmentMetaTest);

 public:
    void Test();
    void TestCompatible();
};

INDEXLIB_UNIT_TEST_CASE(SegmentMetaTest, Test);
INDEXLIB_UNIT_TEST_CASE(SegmentMetaTest, TestCompatible);
void SegmentMetaTest::TestCompatible() {
    string content =
        R"({
    "docid_remap_file_size": "185090256",
    "size": "9440450652",
    "timestamp": "20191031015739",
    "total_doc_num": "19886395",
    "type": "index",
    "valid_doc_num": "15424188"})";

    cout << content << endl;

    const auto &dir = GET_PARTITION_DIRECTORY()->MakeDirectory("meta0");
    auto writer = IndexlibIoWrapper::CreateWriter(dir, "aitheta.segment.meta");
    writer->Write(content.c_str(), content.size());
    writer->Close();
    SegmentMeta meta0;
    meta0.Load(dir);
    EXPECT_EQ(SegmentType::kIndex, meta0.getType());
    EXPECT_EQ(15424188u, meta0.getValidDocNum());
    EXPECT_EQ(19886395u, meta0.getTotalDocNum());
    EXPECT_EQ(0u, meta0.getCategoryNum());
    EXPECT_EQ(0u, meta0.getMaxCategoryDocNum());
    EXPECT_EQ(9440450652u, meta0.getFileSize());
    EXPECT_EQ(false, meta0.Empty());
}

void SegmentMetaTest::Test() {
    SegmentMeta meta0;
    // test default
    EXPECT_EQ(SegmentType::kUnknown, meta0.getType());
    EXPECT_EQ(0u, meta0.getValidDocNum());
    EXPECT_EQ(0u, meta0.getTotalDocNum());
    EXPECT_EQ(0u, meta0.getCategoryNum());
    EXPECT_EQ(0u, meta0.getMaxCategoryDocNum());
    EXPECT_EQ(0u, meta0.getFileSize());
    EXPECT_EQ(true, meta0.Empty());

    meta0.setType(SegmentType::kIndex);
    meta0.setValidDocNum(1024u);
    meta0.setTotalDocNum(4096u);
    meta0.setCategoryNum(8u);
    meta0.setMaxCategoryDocNum(128u);
    meta0.setFileSize(8196u);

    const auto &dir = GET_PARTITION_DIRECTORY()->MakeDirectory("meta0");
    ASSERT_TRUE(meta0.Dump(dir));
    // test load
    SegmentMeta loadMeta;
    ASSERT_TRUE(loadMeta.Load(dir));
    EXPECT_EQ(SegmentType::kIndex, loadMeta.getType());
    EXPECT_EQ(meta0.getValidDocNum(), loadMeta.getValidDocNum());
    EXPECT_EQ(meta0.getTotalDocNum(), loadMeta.getTotalDocNum());
    EXPECT_EQ(meta0.getCategoryNum(), loadMeta.getCategoryNum());
    EXPECT_EQ(meta0.getMaxCategoryDocNum(), loadMeta.getMaxCategoryDocNum());
    EXPECT_EQ(meta0.getFileSize(), loadMeta.getFileSize());
    EXPECT_EQ(false, meta0.Empty());
    // test merge
    SegmentMeta meta1;
    meta1.setType(meta0.getType());
    meta1.setValidDocNum(meta0.getValidDocNum());
    meta1.setTotalDocNum(meta0.getTotalDocNum());
    meta1.setCategoryNum(meta0.getCategoryNum());
    meta1.setMaxCategoryDocNum(0u);
    meta1.setFileSize(meta0.getFileSize());
    meta1.Merge(meta0);

    EXPECT_EQ(SegmentType::kIndex, meta1.getType());
    EXPECT_EQ(meta0.getValidDocNum() * 2, meta1.getValidDocNum());
    EXPECT_EQ(meta0.getTotalDocNum() * 2, meta1.getTotalDocNum());
    EXPECT_EQ(meta0.getCategoryNum() * 2, meta1.getCategoryNum());
    EXPECT_EQ(meta0.getMaxCategoryDocNum(), meta1.getMaxCategoryDocNum());
    EXPECT_EQ(meta0.getFileSize() * 2, meta1.getFileSize());
    EXPECT_EQ(false, meta1.Empty());
}

IE_NAMESPACE_END(aitheta_plugin);
