#ifndef __INDEXLIB_INMEMNORMALINDEXSEGMENTREADERTEST_H
#define __INDEXLIB_INMEMNORMALINDEXSEGMENTREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemNormalIndexSegmentReaderTest : public INDEXLIB_TESTBASE {
public:
    InMemNormalIndexSegmentReaderTest();
    ~InMemNormalIndexSegmentReaderTest();

public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();

private:
    config::IndexConfigPtr mIndexConfig;
    config::FieldSchemaPtr mFieldSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemNormalIndexSegmentReaderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMNORMALINDEXSEGMENTREADERTEST_H
