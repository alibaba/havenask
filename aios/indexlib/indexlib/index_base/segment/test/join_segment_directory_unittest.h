#ifndef __INDEXLIB_JOINSEGMENTDIRECTORYTEST_H
#define __INDEXLIB_JOINSEGMENTDIRECTORYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/segment/join_segment_directory.h"

IE_NAMESPACE_BEGIN(index_base);

class JoinSegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    JoinSegmentDirectoryTest();
    ~JoinSegmentDirectoryTest();

    DECLARE_CLASS_NAME(JoinSegmentDirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddSegment();    
    void TestClone();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(JoinSegmentDirectoryTest, TestAddSegment);
INDEXLIB_UNIT_TEST_CASE(JoinSegmentDirectoryTest, TestClone);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_JOINSEGMENTDIRECTORYTEST_H
