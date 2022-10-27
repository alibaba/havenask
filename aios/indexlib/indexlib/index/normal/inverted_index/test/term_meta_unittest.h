#ifndef __INDEXLIB_TERMMETATEST_H
#define __INDEXLIB_TERMMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

class TermMetaTest : public INDEXLIB_TESTBASE {
public:
    TermMetaTest();
    ~TermMetaTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestEqual();
private:
    std::string mRootDir;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TermMetaTest, TestEqual);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TERMMETATEST_H
