#ifndef __INDEXLIB_POSITIONLISTFORMATOPTIONTEST_H
#define __INDEXLIB_POSITIONLISTFORMATOPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"

IE_NAMESPACE_BEGIN(index);

class PositionListFormatOptionTest : public INDEXLIB_TESTBASE {
public:
    PositionListFormatOptionTest();
    ~PositionListFormatOptionTest();
public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestInit();
    void TestJsonize();

private:
    void InnerTestInitPositionListMeta(
            optionflag_t optionFlag, 
            bool expectHasPosList,
            bool expectHasPosPayload,
            bool expectHasTfBitmap);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PositionListFormatOptionTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PositionListFormatOptionTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(PositionListFormatOptionTest, TestJsonize);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITIONLISTFORMATOPTIONTEST_H
