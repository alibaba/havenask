#ifndef __INDEXLIB_SESSIONFILECACHETEST_H
#define __INDEXLIB_SESSIONFILECACHETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/session_file_cache.h"

IE_NAMESPACE_BEGIN(file_system);

class SessionFileCacheTest : public INDEXLIB_TESTBASE
{
public:
    SessionFileCacheTest();
    ~SessionFileCacheTest();

    DECLARE_CLASS_NAME(SessionFileCacheTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SessionFileCacheTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SESSIONFILECACHETEST_H
