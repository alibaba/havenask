#ifndef __INDEXLIB_INMEMORYATTRIBUTESEGMENTWRITERTEST_H
#define __INDEXLIB_INMEMORYATTRIBUTESEGMENTWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"

IE_NAMESPACE_BEGIN(index);

class InMemoryAttributeSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    InMemoryAttributeSegmentWriterTest();
    ~InMemoryAttributeSegmentWriterTest();

    DECLARE_CLASS_NAME(InMemoryAttributeSegmentWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitWithEmptyParameters();

    void TestAddDocument();
    void TestUpdateDocument();

private:
    void CheckValue(InMemoryAttributeSegmentWriter& writer, 
                    docid_t docId, const std::string& attrName, uint32_t expectValue);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemoryAttributeSegmentWriterTest, TestInitWithEmptyParameters);
INDEXLIB_UNIT_TEST_CASE(InMemoryAttributeSegmentWriterTest, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE(InMemoryAttributeSegmentWriterTest, TestUpdateDocument);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMORYATTRIBUTESEGMENTWRITERTEST_H
