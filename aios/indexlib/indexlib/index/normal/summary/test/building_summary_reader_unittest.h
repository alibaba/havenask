#ifndef __INDEXLIB_BUILDINGSUMMARYREADERTEST_H
#define __INDEXLIB_BUILDINGSUMMARYREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"

IE_NAMESPACE_BEGIN(index);

class BuildingSummaryReaderTest : public INDEXLIB_TESTBASE
{
public:
    BuildingSummaryReaderTest();
    ~BuildingSummaryReaderTest();

    DECLARE_CLASS_NAME(BuildingSummaryReaderTest);
    
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckSummaryReader(LocalDiskSummaryReader& reader,
                            const std::string& pkStr,
                            const std::string& summaryValueStr);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildingSummaryReaderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDINGSUMMARYREADERTEST_H
