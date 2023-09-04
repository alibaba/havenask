#ifndef __INDEXLIB_KVSEGMENTWRITERTEST_H
#define __INDEXLIB_KVSEGMENTWRITERTEST_H

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/partition/segment/kv_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class KVSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    KVSegmentWriterTest();
    ~KVSegmentWriterTest();

    DECLARE_CLASS_NAME(KVSegmentWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestUserTimestamp();
    void TestSwapMmapFile();
    void TestNeedDumpWhenNoSpace();
    void TestMmapFileDump();
    void TestGetTotalMemoryUse();

private:
    void BuildDocs(KVSegmentWriter& writer, const std::string& docStr);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    common::PackAttributeFormatter mFormatter;
    autil::mem_pool::Pool mPool;
    config::KVIndexConfigPtr mKvConfig;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KVSegmentWriterTest, TestUserTimestamp);
INDEXLIB_UNIT_TEST_CASE(KVSegmentWriterTest, TestSwapMmapFile);
INDEXLIB_UNIT_TEST_CASE(KVSegmentWriterTest, TestNeedDumpWhenNoSpace);
INDEXLIB_UNIT_TEST_CASE(KVSegmentWriterTest, TestMmapFileDump);
INDEXLIB_UNIT_TEST_CASE(KVSegmentWriterTest, TestGetTotalMemoryUse);
}} // namespace indexlib::partition

#endif //__INDEXLIB_KVSEGMENTWRITERTEST_H
