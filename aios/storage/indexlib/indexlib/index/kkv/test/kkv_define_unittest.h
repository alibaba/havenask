#ifndef __INDEXLIB_KKVDEFINETEST_H
#define __INDEXLIB_KKVDEFINETEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KkvDefineTest : public INDEXLIB_TESTBASE
{
public:
    KkvDefineTest();
    ~KkvDefineTest();

    DECLARE_CLASS_NAME(KkvDefineTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOnDiskPKeyOffsetCompitable();
    void TestBlockHint();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KkvDefineTest, TestOnDiskPKeyOffsetCompitable);
INDEXLIB_UNIT_TEST_CASE(KkvDefineTest, TestBlockHint);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKVDEFINETEST_H
