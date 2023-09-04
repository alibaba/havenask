#ifndef __INDEXLIB_INMEMNORMALINDEXSEGMENTREADERTEST_H
#define __INDEXLIB_INMEMNORMALINDEXSEGMENTREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class InMemNormalIndexSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    InMemNormalIndexSegmentReaderTest();
    ~InMemNormalIndexSegmentReaderTest();

public:
    void TestSimpleProcess();

private:
    config::IndexConfigPtr mIndexConfig;
    config::FieldSchemaPtr mFieldSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemNormalIndexSegmentReaderTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_INMEMNORMALINDEXSEGMENTREADERTEST_H
