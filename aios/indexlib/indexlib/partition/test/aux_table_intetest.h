#ifndef __INDEXLIB_AUXTABLEINTETEST_H
#define __INDEXLIB_AUXTABLEINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/table_reader_container.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class AuxTableInteTest : public INDEXLIB_TESTBASE
{
public:
    AuxTableInteTest();
    ~AuxTableInteTest();

    DECLARE_CLASS_NAME(AuxTableInteTest);
private:
    struct VirtualAttributeInfo
    {
        VirtualAttributeInfo(const std::string& _mainTableName, 
                             const std::string& _auxTableName,
                             const std::string& _virtualAttributeName,
                             bool _isSub)
            : mainTableName(_mainTableName)
            , auxTableName(_auxTableName)
            , virtualAttributeName(_virtualAttributeName)
            , isSub(_isSub)
            {}
        std::string mainTableName;
        std::string auxTableName;
        std::string virtualAttributeName;
        bool isSub;
    };

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAuxMemoryQuota();
    void TestMultiJoinPartition();
    void TestMultiApplication();
    void TestAddVirtualAttributeFail();
    void TestSameVirtualAttribute();
    void TestUnloadAuxTable();
private:
    void RemoveVirtualAttributeInfo(
        const std::string& mainTableName, const std::string& auxTableName,
        std::vector<VirtualAttributeInfo>& virtualAttrInfos);
    void InnerTestAuxMemoryQuota(util::MemoryQuotaControllerPtr fakeController
                                 = util::MemoryQuotaControllerPtr());
    void InnerTestJoinIndexPartition(
        const partition::JoinRelationMap& joinRelations);
    bool CheckCleanExpiredVirtualAttribute(
        const partition::IndexApplicationPtr& app,
        const std::vector<VirtualAttributeInfo>& expiredVirtualAttributes);
    void AssertQuery(const partition::IndexApplicationPtr& app,
                     const std::vector<int>& queryDocids,
                     const std::vector<int>& expectedDocids,
                     std::vector<VirtualAttributeInfo>& expiredVirtualAttributes,
                     std::string auxTable = "aux",
                     std::string mainTable= "main");
    void AssertPkQuery(const partition::IndexApplicationPtr& app,
                     const std::vector<int>& queryPks,
                     const std::vector<int>& expectedDocids,
                     std::string mainTable);
    void AssertQuery(const partition::IndexApplicationPtr& app,
                     const std::vector<int>& queryDocids,
                     const std::vector<int>& expectedDocids,
                     std::string auxTable = "aux",
                     std::string mainTable= "main")
    {
        std::vector<VirtualAttributeInfo> expiredVirtualAttributes;
        AssertQuery(app, queryDocids, expectedDocids, expiredVirtualAttributes,
                    auxTable, mainTable);
    }

    void QueryFullData(partition::IndexApplicationPtr app,
                       const partition::JoinRelationMap& joinRelations,
                       const std::vector<int>& queryDocids,
                       const std::vector<int>& expectedDocids,
                       std::vector<VirtualAttributeInfo>& expiredVirtualAttributes);

    std::string GetJoinVirtualAttrName(const partition::IndexApplicationPtr& app,
            std::string auxTable = "aux", std::string mainTable= "main");
    config::IndexPartitionSchemaPtr CreateMainSchema(
            std::string tableName, bool hasSub = false);
    config::IndexPartitionSchemaPtr CreateJoinSchema(std::string joinTableName);
    std::string MakeAddCmdString(const std::vector<int>& pks,
                                 const std::vector<int>& attrs,
                                 uint64_t ts);
    std::string MakeMainDocs(const std::vector<int>& pks,
                             uint64_t ts, bool hasSub = true);
    void BuildMainTable(test::PartitionStateMachinePtr psm);
    void BuildAuxTable(test::PartitionStateMachinePtr psm);
    void UpdateMainFull(std::vector<test::PartitionStateMachinePtr>& mainPsms,
                        std::vector<partition::IndexPartitionPtr>& partVec,
                        std::map<std::string, int> &tableName2Part,
                        std::string mainTableName,
                        std::string fullDocs);
    void UpdateAuxFull(std::vector<test::PartitionStateMachinePtr>& auxPsm,
                       std::vector<partition::IndexPartitionPtr>& partVec,
                       std::map<std::string, int> &tableName2Part,
                       std::string joinTableName,
                       std::string fullDocs);
    void ExecuteBuild(const std::vector<test::PartitionStateMachinePtr>& mainPsms,
                      const std::vector<test::PartitionStateMachinePtr>& auxPsms);

private:
    std::string mMainRoot;
    std::string mAuxRoot;
    config::IndexPartitionSchemaPtr mMainSchema;
    config::IndexPartitionSchemaPtr mAuxSchema;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AuxTableInteTest, TestAuxMemoryQuota);
INDEXLIB_UNIT_TEST_CASE(AuxTableInteTest, TestMultiJoinPartition);
INDEXLIB_UNIT_TEST_CASE(AuxTableInteTest, TestMultiApplication);
INDEXLIB_UNIT_TEST_CASE(AuxTableInteTest, TestAddVirtualAttributeFail);
INDEXLIB_UNIT_TEST_CASE(AuxTableInteTest, TestSameVirtualAttribute);
INDEXLIB_UNIT_TEST_CASE(AuxTableInteTest, TestUnloadAuxTable);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_AUXTABLEINTETEST_H
