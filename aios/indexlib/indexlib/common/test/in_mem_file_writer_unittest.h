#ifndef __INDEXLIB_INMEMFILEWRITERTEST_H
#define __INDEXLIB_INMEMFILEWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/in_mem_file_writer.h"

IE_NAMESPACE_BEGIN(common);

class InMemFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    InMemFileWriterTest();
    ~InMemFileWriterTest();

    DECLARE_CLASS_NAME(InMemFileWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemFileWriterTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_INMEMFILEWRITERTEST_H
