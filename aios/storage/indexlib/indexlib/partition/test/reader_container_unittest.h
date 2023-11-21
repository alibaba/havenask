#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class ReaderContainerTest : public INDEXLIB_TESTBASE
{
public:
    ReaderContainerTest();
    ~ReaderContainerTest();

    DECLARE_CLASS_NAME(ReaderContainerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestHasReader();
    void TestGetSwitchRtSegments();
    void TestGetNeedKeepDeployFiles();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ReaderContainerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ReaderContainerTest, TestHasReader);
INDEXLIB_UNIT_TEST_CASE(ReaderContainerTest, TestGetSwitchRtSegments);
INDEXLIB_UNIT_TEST_CASE(ReaderContainerTest, TestGetNeedKeepDeployFiles);
}} // namespace indexlib::partition
