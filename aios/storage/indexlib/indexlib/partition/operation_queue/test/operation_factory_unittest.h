#ifndef __INDEXLIB_OPERATIONFACTORYTEST_H
#define __INDEXLIB_OPERATIONFACTORYTEST_H

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OperationFactoryTest : public INDEXLIB_TESTBASE
{
public:
    OperationFactoryTest();
    ~OperationFactoryTest();

    DECLARE_CLASS_NAME(OperationFactoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

    void TestInit();
    void TestCreateOperationForAddDocument();
    void TestCreateOperationForDeleteDocument();
    void TestCreateOperationForUpdateDocument();

private:
    config::IndexPartitionSchemaPtr CreateSchema(bool hasSub);

    OperationBase* CreateOperation(OperationFactory& factory, const document::NormalDocumentPtr& doc,
                                   autil::mem_pool::Pool* pool);

    void CheckOperationCreator(const config::IndexPartitionSchemaPtr& schema, OperationCreatorPtr operationCreator,
                               const std::string& typeStr);

    void CheckSingleOperationCreator(const config::IndexPartitionSchemaPtr& schema,
                                     OperationCreatorPtr operationCreator, const std::string& typeStr);

    void CheckSubDocOperationCreator(const config::IndexPartitionSchemaPtr& schema,
                                     OperationCreatorPtr operationCreator, const std::string& mainTypeStr,
                                     const std::string& subTypeStr);

    void CheckOperationType(OperationBase* oper, const std::string& typeStr);

    void CheckSingleOperationType(OperationBase* oper, const std::string& typeStr);

    void CheckSubDocOperationType(OperationBase* oper, const std::string& mainTypeStr, const std::string& subTypeStr);

    // TODO: uint128 case

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationFactoryTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationFactoryTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(OperationFactoryTest, TestCreateOperationForAddDocument);
INDEXLIB_UNIT_TEST_CASE(OperationFactoryTest, TestCreateOperationForDeleteDocument);
INDEXLIB_UNIT_TEST_CASE(OperationFactoryTest, TestCreateOperationForUpdateDocument);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATIONFACTORYTEST_H
