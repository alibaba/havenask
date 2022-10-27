#ifndef __INDEXLIB_ATTRIBUTEBIGDATATEST_H
#define __INDEXLIB_ATTRIBUTEBIGDATATEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(index);

class AttributeBigDataTest : public INDEXLIB_TESTBASE
{
public:
    AttributeBigDataTest();
    ~AttributeBigDataTest();

    DECLARE_CLASS_NAME(AttributeBigDataTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSingleValueAttrWithBigDataInOneSegmentLongCaseTest();
    void TestSingleValueAttrWithBigDataInOneSegmentEqualCompressLongCaseTest();
private:
    template<typename T>
    void DoTestSingleValueAttrWithBigDataInOneSegment(const std::string& fieldType,
                                                      bool isEqualCompress = false);
    void MakeSchema(const std::string& fieldType, bool isEqualCompress);
    file_system::DirectoryPtr MakePartition(uint32_t docCount);
private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    config::AttributeConfigPtr mAttrConfig;
    file_system::DirectoryPtr mPartDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeBigDataTest, TestSingleValueAttrWithBigDataInOneSegmentLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(AttributeBigDataTest, TestSingleValueAttrWithBigDataInOneSegmentEqualCompressLongCaseTest);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEBIGDATATEST_H
