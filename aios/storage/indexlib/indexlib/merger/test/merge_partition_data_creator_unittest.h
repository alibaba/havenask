#ifndef __INDEXLIB_MERGEPARTITIONDATACREATORTEST_H
#define __INDEXLIB_MERGEPARTITIONDATACREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MergePartitionDataCreatorTest : public INDEXLIB_TESTBASE
{
public:
    MergePartitionDataCreatorTest();
    ~MergePartitionDataCreatorTest();

    DECLARE_CLASS_NAME(MergePartitionDataCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateMergePartitionData();

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mField;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergePartitionDataCreatorTest, TestCreateMergePartitionData);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGEPARTITIONDATACREATORTEST_H
