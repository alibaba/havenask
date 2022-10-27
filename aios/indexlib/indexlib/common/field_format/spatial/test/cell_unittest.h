#ifndef __INDEXLIB_CELLTEST_H
#define __INDEXLIB_CELLTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/cell.h"

IE_NAMESPACE_BEGIN(common);

class CellTest : public INDEXLIB_TESTBASE
{
public:
    CellTest();
    ~CellTest();

    DECLARE_CLASS_NAME(CellTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetSubCells();
    void TestGetCellRectangle();
private:
    void InnerTestGetCellRectangle(const std::string& cellStr, int8_t level);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CellTest, TestGetSubCells);
INDEXLIB_UNIT_TEST_CASE(CellTest, TestGetCellRectangle);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CELLTEST_H
