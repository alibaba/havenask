#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_writer.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib { namespace index {

class RangeIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    RangeIndexWriterTest();
    ~RangeIndexWriterTest();

    DECLARE_CLASS_NAME(RangeIndexWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RangeIndexWriterTest, TestSimpleProcess);
IE_LOG_SETUP(index, RangeIndexWriterTest);

RangeIndexWriterTest::RangeIndexWriterTest() {}

RangeIndexWriterTest::~RangeIndexWriterTest() {}

void RangeIndexWriterTest::CaseSetUp() {}

void RangeIndexWriterTest::CaseTearDown() {}

void RangeIndexWriterTest::TestSimpleProcess()
{
    double expandRatio = 1.3;
    double bottomRatio = 0.8;
    double highRatio = 0.2;

    config::IndexPartitionOptions options;
    string indexName = "range_index";
    std::shared_ptr<framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics);

    auto CheckDistinctTermCount = [&](double lastHighTermCount, double lastBottomTermCount) {
        RangeIndexWriter writer(indexName, segmentMetrics, options);
        size_t expectBottomTermCount = (size_t)(lastBottomTermCount * expandRatio) / 10 * 10;
        size_t expectHighTermCount = (size_t)(lastHighTermCount * expandRatio) / 10 * 10;

        size_t actureBottomTermCount = writer.mBottomLevelWriter->GetHashMapInitSize() / 10 * 10;
        size_t actureHighTermCount = writer.mHighLevelWriter->GetHashMapInitSize() / 10 * 10;
        ASSERT_EQ(expectBottomTermCount, actureBottomTermCount);
        ASSERT_EQ(expectHighTermCount, actureHighTermCount);
    };

    CheckDistinctTermCount(HASHMAP_INIT_SIZE * 10 * highRatio, HASHMAP_INIT_SIZE * 10 * bottomRatio);

    size_t initHashMapSize = 123456;
    segmentMetrics->SetDistinctTermCount(indexName, initHashMapSize);
    CheckDistinctTermCount(initHashMapSize * highRatio, initHashMapSize * bottomRatio);

    size_t highInitHashMapSize = 2345;
    size_t bottomInitHashMapSize = 12357;
    segmentMetrics->SetDistinctTermCount(config::RangeIndexConfig::GetHighLevelIndexName(indexName),
                                         highInitHashMapSize);
    segmentMetrics->SetDistinctTermCount(config::RangeIndexConfig::GetBottomLevelIndexName(indexName),
                                         bottomInitHashMapSize);
    CheckDistinctTermCount(highInitHashMapSize, bottomInitHashMapSize);
}

}} // namespace indexlib::index
