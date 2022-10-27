#ifndef __INDEXLIB_ONLINESEGMENTDIRECTORYTEST_H
#define __INDEXLIB_ONLINESEGMENTDIRECTORYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/segment/online_segment_directory.h"

IE_NAMESPACE_BEGIN(index_base);

class OnlineSegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    OnlineSegmentDirectoryTest();
    ~OnlineSegmentDirectoryTest();

    DECLARE_CLASS_NAME(OnlineSegmentDirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestConvertToRtSegmentId();
    void TestConvertToJoinSegmentId();
    void TestInitTwice();
    void TestRefreshVersion();
    void TestAddSegment();
    void TestInitSub();
    void TestInitSubForEmptyVersion();
    void TestRemoveSegments();
    void TestSwitchToLinkDirectoryForRtSegments();

private:
    void InnerTestSwitchToLinkDirectoryForRtSegments(bool flushRt);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestConvertToRtSegmentId);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestConvertToJoinSegmentId);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestInitTwice);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestRefreshVersion);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestAddSegment);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestInitSub);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestInitSubForEmptyVersion);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestRemoveSegments);
INDEXLIB_UNIT_TEST_CASE(OnlineSegmentDirectoryTest, TestSwitchToLinkDirectoryForRtSegments);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_ONLINESEGMENTDIRECTORYTEST_H
