#ifndef __INDEXLIB_BUILTKKVSEGMENTREADERTEST_H
#define __INDEXLIB_BUILTKKVSEGMENTREADERTEST_H

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/built_kkv_segment_reader.h"
#include "indexlib/partition/segment/multi_region_kkv_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class BuiltKKVSegmentReaderTest : public INDEXLIB_TESTBASE_WITH_PARAM<PKeyTableType>
{
public:
    BuiltKKVSegmentReaderTest();
    ~BuiltKKVSegmentReaderTest();

    DECLARE_CLASS_NAME(BuiltKKVSegmentReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMultiRegion();

private:
    void BuildDocs(partition::KKVSegmentWriter<uint64_t>& writer, const config::IndexPartitionSchemaPtr& schema,
                   const std::string& docStr);

    void CheckData(const index_base::SegmentData& segData, const config::KKVIndexConfigPtr& kkvConfig,
                   const index::KKVIndexOptions& kkvIndexOptions, const std::string& pkeyStr,
                   const std::string& skeyStr, const std::string& expectedValues, const std::string& expectedTs);

private:
    std::shared_ptr<autil::mem_pool::Pool> mPool;
    util::QuotaControlPtr mQuotaControl;
    future_lite::executors::SimpleExecutor mEx;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(p1, BuiltKKVSegmentReaderTest,
                        testing::Values(index::PKT_DENSE, index::PKT_CUCKOO, index::PKT_SEPARATE_CHAIN));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuiltKKVSegmentReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BuiltKKVSegmentReaderTest, TestMultiRegion);
}} // namespace indexlib::index

#endif //__INDEXLIB_BUILTKKVSEGMENTREADERTEST_H
