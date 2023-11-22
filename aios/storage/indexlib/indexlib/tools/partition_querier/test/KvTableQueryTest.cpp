#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/testutil/unittest.h"
#include "tools/partition_querier/executors/KvTableExecutor.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::testlib;
using namespace indexlibv2::base;
using namespace google::protobuf;

namespace indexlib { namespace tools {

class KvTableQueryTest : public INDEXLIB_TESTBASE
{
public:
    KvTableQueryTest();
    ~KvTableQueryTest();

    IndexPartitionPtr createKvPartition(string tableName, PsmEvent build_event = BUILD_FULL);

    DECLARE_CLASS_NAME(KvTableQueryTest);

public:
    void TestReadAttrWithPk();
    void TestQueryNotExistPK();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KvTableQueryTest, TestReadAttrWithPk);
INDEXLIB_UNIT_TEST_CASE(KvTableQueryTest, TestQueryNotExistPK);

AUTIL_LOG_SETUP(indexlib.tools, KvTableQueryTest);

KvTableQueryTest::KvTableQueryTest() {}
KvTableQueryTest::~KvTableQueryTest() {}

IndexPartitionPtr KvTableQueryTest::createKvPartition(string tableName, PsmEvent build_event)

{
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/kv_" + tableName;
    auto schemaPtr = IndexlibPartitionCreator::CreateKVSchema(
        tableName,
        "key:uint64;attr1:int32;attr2:float;attr3:int32:true;attr4:float:true;attr5:string;attr6:string:true;"
        "attr7:int16:true:true::5;attr8:float:true:true::6",
        "key", "attr1;attr2;attr3;attr4;attr5;attr6;attr7;attr8");
    string docStr = "cmd=add,key=123456,"
                    "attr1=2,"
                    "attr2=3.3,"
                    "attr3=11 12,"
                    "attr4=22.1 22.2 22.3,"
                    "attr5=7.7,"
                    "attr6=8.1 8.2 8.3 8.4,"
                    "attr7=9 10 9 10 8,"
                    "attr8=9.1 9.2 9.3 9.4 9.5 9.6;"
                    "cmd=add,key=456789,"
                    "attr1=4,"
                    "attr2=5.5,"
                    "attr3=41 42,"
                    "attr4=66.1 66.2 66.3,"
                    "attr5=9.9,"
                    "attr6=10.1 10.2 10.3 10.4,"
                    "attr7=29 20 29 20 28,"
                    "attr8=29.1 29.2 29.3 29.4 29.5 29.6;";
    return IndexlibPartitionCreator::CreateIndexPartition(schemaPtr, indexRoot, docStr);
}

void KvTableQueryTest::TestReadAttrWithPk()
{
    {
        auto indexPartition = createKvPartition("kv_table_1");
        auto indexPartitionReader = indexPartition->GetReader();
        PartitionQuery request;
        request.add_pk("123456");
        request.add_attrs("attr1");
        request.add_attrs("attr4");
        PartitionResponse response;
        const auto status = KvTableExecutor::QueryKVTable(indexPartitionReader, DEFAULT_REGIONID, request, response);
        EXPECT_TRUE(status.IsOK());
        EXPECT_EQ(1, response.rows_size());
        EXPECT_EQ("123456", response.rows(0).pk());
        EXPECT_EQ(2, response.rows(0).attrvalues_size());
        EXPECT_EQ(ValueType::INT_32, response.rows(0).attrvalues(0).type());
        EXPECT_EQ(ValueType::FLOAT, response.rows(0).attrvalues(1).type());
        EXPECT_EQ(2, response.rows(0).attrvalues(0).int32_value());
        EXPECT_EQ(3, response.rows(0).attrvalues(1).multi_float_value().value_size());
        EXPECT_NEAR(22.1, response.rows(0).attrvalues(1).multi_float_value().value(0), 0.00001);
        EXPECT_NEAR(22.2, response.rows(0).attrvalues(1).multi_float_value().value(1), 0.00001);
        EXPECT_NEAR(22.3, response.rows(0).attrvalues(1).multi_float_value().value(2), 0.00001);
    }

    {
        auto indexPartition = createKvPartition("kv_table_2");
        auto indexPartitionReader = indexPartition->GetReader();
        PartitionQuery request;
        request.add_pk("456789");
        request.add_attrs("attr1");
        request.add_attrs("attr4");
        PartitionResponse response;
        const auto status = KvTableExecutor::QueryKVTable(indexPartitionReader, DEFAULT_REGIONID, request, response);
        EXPECT_TRUE(status.IsOK());
        EXPECT_EQ(1, response.rows_size());
        EXPECT_EQ("456789", response.rows(0).pk());
        EXPECT_EQ(2, response.rows(0).attrvalues_size());
        EXPECT_EQ(ValueType::INT_32, response.rows(0).attrvalues(0).type());
        EXPECT_EQ(ValueType::FLOAT, response.rows(0).attrvalues(1).type());
        EXPECT_EQ(4, response.rows(0).attrvalues(0).int32_value());
        EXPECT_EQ(3, response.rows(0).attrvalues(1).multi_float_value().value_size());
        EXPECT_NEAR(66.1, response.rows(0).attrvalues(1).multi_float_value().value(0), 0.00001);
        EXPECT_NEAR(66.2, response.rows(0).attrvalues(1).multi_float_value().value(1), 0.00001);
        EXPECT_NEAR(66.3, response.rows(0).attrvalues(1).multi_float_value().value(2), 0.00001);
    }

    {
        auto indexPartition = createKvPartition("kv_table_3");
        auto indexPartitionReader = indexPartition->GetReader();
        PartitionQuery request;
        request.add_pk("123456");
        PartitionResponse response;
        const auto status = KvTableExecutor::QueryKVTable(indexPartitionReader, DEFAULT_REGIONID, request, response);
        EXPECT_TRUE(status.IsOK());
        EXPECT_EQ(1, response.rows_size());
        EXPECT_EQ("123456", response.rows(0).pk());
        EXPECT_EQ(8, response.rows(0).attrvalues_size());
    }

    {
        auto indexPartition = createKvPartition("kv_table_4");
        auto indexPartitionReader = indexPartition->GetReader();
        PartitionQuery request;
        request.add_pk("123456");
        request.add_attrs("attr6");
        PartitionResponse response;
        const auto status = KvTableExecutor::QueryKVTable(indexPartitionReader, DEFAULT_REGIONID, request, response);
        EXPECT_TRUE(status.IsOK());
        EXPECT_EQ(1, response.rows_size());
        EXPECT_EQ("123456", response.rows(0).pk());
        EXPECT_EQ(1, response.rows(0).attrvalues_size());
        EXPECT_EQ(ValueType::STRING, response.rows(0).attrvalues(0).type());
        EXPECT_EQ(4, response.rows(0).attrvalues(0).multi_bytes_value().value_size());
        EXPECT_EQ("8.1", response.rows(0).attrvalues(0).multi_bytes_value().value(0));
        EXPECT_EQ("8.2", response.rows(0).attrvalues(0).multi_bytes_value().value(1));
        EXPECT_EQ("8.3", response.rows(0).attrvalues(0).multi_bytes_value().value(2));
        EXPECT_EQ("8.4", response.rows(0).attrvalues(0).multi_bytes_value().value(3));
    }
}

void KvTableQueryTest::TestQueryNotExistPK()
{
    auto indexPartition = createKvPartition("kv_table");
    auto indexPartitionReader = indexPartition->GetReader();
    PartitionQuery request;
    request.add_pk("not exist");
    PartitionResponse response;
    const auto status = KvTableExecutor::QueryKVTable(indexPartitionReader, DEFAULT_REGIONID, request, response);
    EXPECT_TRUE(status.IsNotFound());
}

}} // namespace indexlib::tools
