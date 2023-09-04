#ifndef __INDEXLIB_UNCOMPRESSOFFSETREADERTEST_H
#define __INDEXLIB_UNCOMPRESSOFFSETREADERTEST_H

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/uncompress_offset_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class UncompressOffsetReaderTest : public INDEXLIB_TESTBASE
{
public:
    UncompressOffsetReaderTest();
    ~UncompressOffsetReaderTest();

    DECLARE_CLASS_NAME(UncompressOffsetReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInit();
    void TestExtendOffsetVector();

private:
    void InnerTestExtendOffsetVector(uint32_t docCount);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UncompressOffsetReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(UncompressOffsetReaderTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(UncompressOffsetReaderTest, TestExtendOffsetVector);
}} // namespace indexlib::index

#endif //__INDEXLIB_UNCOMPRESSOFFSETREADERTEST_H
