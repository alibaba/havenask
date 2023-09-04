#ifndef __INDEXLIB_INDEXPATHUTILTEST_H
#define __INDEXLIB_INDEXPATHUTILTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/index_path_util.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class IndexPathUtilTest : public INDEXLIB_TESTBASE
{
public:
    IndexPathUtilTest();
    ~IndexPathUtilTest();

    DECLARE_CLASS_NAME(IndexPathUtilTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetSegmentId();
    void TestGetVersionId();
    void TestGetDeployMetaId();
    void TestGetPatchMetaId();
    void TestGetSchemaId();
    void TestGetPatchIndexId();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexPathUtilTest, TestGetSegmentId);
INDEXLIB_UNIT_TEST_CASE(IndexPathUtilTest, TestGetVersionId);
INDEXLIB_UNIT_TEST_CASE(IndexPathUtilTest, TestGetDeployMetaId);
INDEXLIB_UNIT_TEST_CASE(IndexPathUtilTest, TestGetPatchMetaId);
INDEXLIB_UNIT_TEST_CASE(IndexPathUtilTest, TestGetSchemaId);
INDEXLIB_UNIT_TEST_CASE(IndexPathUtilTest, TestGetPatchIndexId);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_INDEXPATHUTILTEST_H
