#ifndef __INDEXLIB_SKIPLISTREADERTEST_H
#define __INDEXLIB_SKIPLISTREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"

IE_NAMESPACE_BEGIN(index);

class SkipListReaderTest : public INDEXLIB_TESTBASE
{
public:
    SkipListReaderTest();
    ~SkipListReaderTest();

    DECLARE_CLASS_NAME(SkipListReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SkipListReaderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SKIPLISTREADERTEST_H
