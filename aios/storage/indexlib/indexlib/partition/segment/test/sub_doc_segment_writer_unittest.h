#ifndef __INDEXLIB_SUBDOCSEGMENTWRITERTEST_H
#define __INDEXLIB_SUBDOCSEGMENTWRITERTEST_H
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class SubDocSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    SubDocSegmentWriterTest();
    ~SubDocSegmentWriterTest();

    DECLARE_CLASS_NAME(SubDocSegmentWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAddDocsFail();
    void TestAllocateBuildResourceMetricsNode();
    void TestEstimateInitMemUse();

private:
    config::IndexPartitionSchemaPtr _schema;
    autil::mem_pool::Pool _pool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestAddDocsFail);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestAllocateBuildResourceMetricsNode);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestEstimateInitMemUse);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SUBDOCSEGMENTWRITERTEST_H
