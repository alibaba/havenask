#ifndef __INDEXLIB_ATTRIBUTEREADERBLOCKCACHETEST_H
#define __INDEXLIB_ATTRIBUTEREADERBLOCKCACHETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(index);

class AttributeReaderBlockCacheTest : public INDEXLIB_TESTBASE
{
public:
    AttributeReaderBlockCacheTest();
    ~AttributeReaderBlockCacheTest();

    DECLARE_CLASS_NAME(AttributeReaderBlockCacheTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSingleValue();
private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeReaderBlockCacheTest, TestSingleValue);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEREADERBLOCKCACHETEST_H
