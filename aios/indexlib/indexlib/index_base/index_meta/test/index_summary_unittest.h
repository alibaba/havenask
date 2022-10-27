#ifndef __INDEXLIB_INDEXSUMMARYTEST_H
#define __INDEXLIB_INDEXSUMMARYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/index_summary.h"

IE_NAMESPACE_BEGIN(index_base);

class IndexSummaryTest : public INDEXLIB_TESTBASE
{
public:
    IndexSummaryTest();
    ~IndexSummaryTest();

    DECLARE_CLASS_NAME(IndexSummaryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    // fileInfos : fileName:len;...
    void MakeSegmentData(const std::string& rootDir,
                         segmentid_t id, const std::string& fileInfos);

    void MakeVersion(const std::string& rootDir,
                     const std::string& segIds, versionid_t id,
                     bool createDeployIndex);

    void CheckIndexSummary(const IndexSummary& ret, const std::string& usingSegIds,
                           const std::string& totalSegIds, int64_t totalSize);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexSummaryTest, TestSimpleProcess);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INDEXSUMMARYTEST_H
