#ifndef __INDEXLIB_DATEINDEXSEGMENTREADERTEST_H
#define __INDEXLIB_DATEINDEXSEGMENTREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class DateIndexSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    DateIndexSegmentReaderTest();
    ~DateIndexSegmentReaderTest();

    DECLARE_CLASS_NAME(DateIndexSegmentReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormalizeTimestamp();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateIndexSegmentReaderTest, TestNormalizeTimestamp);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DATEINDEXSEGMENTREADERTEST_H
