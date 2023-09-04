#ifndef __INDEXLIB_INDEXSUMMARYTEST_H
#define __INDEXLIB_INDEXSUMMARYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

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
    void MakeSegmentData(const indexlib::file_system::DirectoryPtr& rootDir, segmentid_t id,
                         const std::string& fileInfos);

    void MakeVersion(const indexlib::file_system::DirectoryPtr& rootDir, const std::string& segIds, versionid_t id,
                     bool createDeployIndex);

    void CheckIndexSummary(const IndexSummary& ret, const std::string& usingSegIds, const std::string& totalSegIds,
                           int64_t totalSize);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexSummaryTest, TestSimpleProcess);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_INDEXSUMMARYTEST_H
