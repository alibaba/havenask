#ifndef __INDEXLIB_PROGRESSSYNCHRONIZERTEST_H
#define __INDEXLIB_PROGRESSSYNCHRONIZERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"

IE_NAMESPACE_BEGIN(index_base);

class ProgressSynchronizerTest : public INDEXLIB_TESTBASE
{
public:
    ProgressSynchronizerTest();
    ~ProgressSynchronizerTest();

    DECLARE_CLASS_NAME(ProgressSynchronizerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    // str: ts,src,offset;ts,src,offset
    void MakeVersions(const std::string& str,
                      std::vector<Version>& versions);

    // str: ts,src,offset;ts,src,offset
    void MakeSegmentInfos(const std::string& str,
                          std::vector<SegmentInfo>& segInfos);

    void CheckLocator(const ProgressSynchronizer& ps,
                      int64_t src, int64_t offset);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ProgressSynchronizerTest, TestSimpleProcess);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PROGRESSSYNCHRONIZERTEST_H
