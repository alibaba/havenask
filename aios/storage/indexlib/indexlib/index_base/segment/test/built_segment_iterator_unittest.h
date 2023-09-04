#ifndef __INDEXLIB_BUILTSEGMENTITERATORTEST_H
#define __INDEXLIB_BUILTSEGMENTITERATORTEST_H

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/built_segment_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class BuiltSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    BuiltSegmentIteratorTest();
    ~BuiltSegmentIteratorTest();

    DECLARE_CLASS_NAME(BuiltSegmentIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckIterator(BuiltSegmentIterator& iter, const std::string& inMemInfoStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuiltSegmentIteratorTest, TestSimpleProcess);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_BUILTSEGMENTITERATORTEST_H
