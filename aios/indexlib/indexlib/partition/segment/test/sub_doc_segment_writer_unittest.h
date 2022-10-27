#ifndef __INDEXLIB_SUBDOCSEGMENTWRITERTEST_H
#define __INDEXLIB_SUBDOCSEGMENTWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

IE_NAMESPACE_BEGIN(partition);

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
    void TestDedupSubDoc();
    void TestAddJoinFieldToDocument();
    void TestAddDocsFail();
    void TestAllocateBuildResourceMetricsNode();
    void TestEstimateInitMemUse();
private:
    SubDocSegmentWriterPtr PrepareSubSegmentWriter();
    void RemoveAttributeDocument(document::NormalDocumentPtr& doc);
    void CheckJoinFieldInDoc(
            const document::NormalDocumentPtr& doc, docid_t subJoinValue);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    index_base::InMemorySegmentPtr mInMemSeg;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestDedupSubDoc);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestAddJoinFieldToDocument);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestAddDocsFail);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestAllocateBuildResourceMetricsNode);
INDEXLIB_UNIT_TEST_CASE(SubDocSegmentWriterTest, TestEstimateInitMemUse);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUBDOCSEGMENTWRITERTEST_H
