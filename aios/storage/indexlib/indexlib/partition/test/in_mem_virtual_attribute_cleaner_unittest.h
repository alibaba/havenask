#ifndef __INDEXLIB_INMEMVIRTUALATTRIBUTECLEANERTEST_H
#define __INDEXLIB_INMEMVIRTUALATTRIBUTECLEANERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/in_mem_virtual_attribute_cleaner.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class InMemVirtualAttributeCleanerTest : public INDEXLIB_TESTBASE
{
public:
    InMemVirtualAttributeCleanerTest();
    ~InMemVirtualAttributeCleanerTest();

    DECLARE_CLASS_NAME(InMemVirtualAttributeCleanerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void MakeAttributeDir(const file_system::DirectoryPtr& directory, const std::string& segName,
                          const std::string& attrName, bool isVirtual);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemVirtualAttributeCleanerTest, TestSimpleProcess);
}} // namespace indexlib::partition

#endif //__INDEXLIB_INMEMVIRTUALATTRIBUTECLEANERTEST_H
