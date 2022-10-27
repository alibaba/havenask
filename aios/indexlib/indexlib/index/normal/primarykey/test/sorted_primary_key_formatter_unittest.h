#ifndef __INDEXLIB_SORTEDPRIMARYKEYFORMATTERTEST_H
#define __INDEXLIB_SORTEDPRIMARYKEYFORMATTERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"

IE_NAMESPACE_BEGIN(index);

class SortedPrimaryKeyFormatterTest : public INDEXLIB_TESTBASE
{
public:
    SortedPrimaryKeyFormatterTest();
    ~SortedPrimaryKeyFormatterTest();

    DECLARE_CLASS_NAME(SortedPrimaryKeyFormatterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFind();
    void TestSerializeFromHashMap();
    void TestDeserializeToSliceFile();
private:
    void CheckSliceFile(const file_system::SliceFilePtr& sliceFile,
                        const std::string& docs, const std::string& expectDocids);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestFind);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestSerializeFromHashMap);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestDeserializeToSliceFile);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORTEDPRIMARYKEYFORMATTERTEST_H
