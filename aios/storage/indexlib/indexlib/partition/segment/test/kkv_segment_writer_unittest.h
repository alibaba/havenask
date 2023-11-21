#pragma once

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/partition/segment/kkv_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class KKVSegmentWriterTest : public INDEXLIB_TESTBASE_WITH_PARAM<index::PKeyTableType>
{
public:
    KKVSegmentWriterTest();
    ~KKVSegmentWriterTest();

    DECLARE_CLASS_NAME(KKVSegmentWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestNeedForceDumpForDenseTable();
    void TestConfigWorkForSuffixKeyWriter();
    void TestUserTimestamp();
    void TestSwapMmapFileWriter();
    void TestNeedDumpWhenNoSpace();
    void TestMmapFileDump();
    void TestCalculateMemoryRatio();
    void TestBuildSKeyFull();

private:
    void CheckReader(const index_base::InMemorySegmentReaderPtr& reader, const std::string& pkeyStr,
                     const std::string& skeyStr, const std::string& expectedValues, const std::string& expectedTs);
    void BuildDocs(KKVSegmentWriter<uint64_t>& writer, const std::string& docStr);
    size_t CalcucateSize(size_t docCount, size_t pkeyCount, const config::IndexPartitionOptions& options,
                         const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics,
                         bool useSwapMmapFile = false);

private:
    config::IndexPartitionSchemaPtr mSchema;
    common::PackAttributeFormatter mFormatter;
    autil::mem_pool::Pool mPool;
    config::KKVIndexConfigPtr mKkvConfig;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(p1, KKVSegmentWriterTest, testing::Values(index::PKT_DENSE, index::PKT_SEPARATE_CHAIN));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestNeedForceDumpForDenseTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestConfigWorkForSuffixKeyWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestUserTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestSwapMmapFileWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestNeedDumpWhenNoSpace);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestMmapFileDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestCalculateMemoryRatio);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(KKVSegmentWriterTest, TestBuildSKeyFull);
}} // namespace indexlib::partition
