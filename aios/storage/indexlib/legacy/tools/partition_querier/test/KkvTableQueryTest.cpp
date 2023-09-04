#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/legacy/tools/partition_querier/executors/KkvTableExecutor.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/testutil/unittest.h"

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

class KkvTableQueryTest : public INDEXLIB_TESTBASE
{
public:
    KkvTableQueryTest();
    ~KkvTableQueryTest();

    IndexPartitionPtr createKkvPartition(const string tableName);
    IndexPartitionPtr createStringKkvPartition(const string tableName);

private:
    void prepareKkvTableData(std::string& fields, std::string& pkeyField, std::string& skeyField,
                             std::string& attributes, std::string& docs, int64_t& ttl);

    void prepareKkvTableStringData(std::string& fields, std::string& pkeyField, std::string& skeyField,
                                   std::string& attributes, std::string& docs, int64_t& ttl);

public:
    void TestReadKkvTable();
    void TestQueryWithSkInAttr();
    void TestReadKkvTableWithEmptySk();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KkvTableQueryTest, TestReadKkvTable);
INDEXLIB_UNIT_TEST_CASE(KkvTableQueryTest, TestQueryWithSkInAttr);
INDEXLIB_UNIT_TEST_CASE(KkvTableQueryTest, TestReadKkvTableWithEmptySk);

AUTIL_LOG_SETUP(indexlib.tools, KkvTableQueryTest);

KkvTableQueryTest::KkvTableQueryTest() {}
KkvTableQueryTest::~KkvTableQueryTest() {}

void KkvTableQueryTest::prepareKkvTableData(std::string& fields, std::string& pkeyField, std::string& skeyField,
                                            std::string& attributes, std::string& docs, int64_t& ttl)
{
    fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true;attr3:float;attr4:float:true;attr5:"
             "int16:true:true::3;attr6:float:true:true::3";
    pkeyField = "trigger_id";
    skeyField = "sk";
    attributes = "sk;attr2;name;value;attr3;attr4;attr5;attr6";
    docs = "cmd=add,sk=0,attr2=0,trigger_id=0,name=str1,value=111 aaa,attr3=1.1,attr4=1.1 1.2,attr5=60 70 80,attr6=6.0 "
           "7.0 8.0,ts=10000000;"
           "cmd=add,sk=1,attr2=0,trigger_id=0,name=str2,value=222 bbb,attr3=2.1,attr4=2.1 2.2,attr5=61 71 81,attr6=6.1 "
           "7.1 8.1,ts=20000000;"
           "cmd=add,sk=0,attr2=2,trigger_id=1,name=str3,value=333 ccc,attr3=3.1,attr4=3.1 3.2,attr5=62 72 82,attr6=6.2 "
           "7.2 8.2,ts=30000000;"
           "cmd=add,sk=1,attr2=2,trigger_id=1,name=str4,value=444 ddd,attr3=4.1,attr4=4.1 4.2,attr5=63 73 83,attr6=6.3 "
           "7.3 8.3,ts=40000000;"
           "cmd=add,sk=2,attr2=2,trigger_id=1,name=str5,value=555 eee,attr3=5.1,attr4=5.1 5.2,attr5=64 74 84,attr6=6.4 "
           "7.4 8.4,ts=50000000;"
           "cmd=add,sk=0,attr2=2,trigger_id=2,name=str6,value=666 fff,attr3=6.1,attr4=6.1 6.2,attr5=65 75 85,attr6=6.5 "
           "7.5 8.5,ts=60000000";
    ttl = std::numeric_limits<int64_t>::max();
}

void KkvTableQueryTest::prepareKkvTableStringData(std::string& fields, std::string& pkeyField, std::string& skeyField,
                                                  std::string& attributes, std::string& docs, int64_t& ttl)
{
    fields = "sk:string;attr2:uint32;trigger_id:string;name:string;value:string:true";
    pkeyField = "trigger_id";
    skeyField = "sk";
    attributes = "sk;attr2;name;value";
    docs = "cmd=add,sk=0,attr2=0,trigger_id=0,name=str1,value=111 aaa,ts=10000000;"
           "cmd=add,sk=1,attr2=0,trigger_id=0,name=str2,value=222 bbb,ts=20000000;"
           "cmd=add,sk=0,attr2=2,trigger_id=1,name=str3,value=333 ccc,ts=30000000;"
           "cmd=add,sk=1,attr2=2,trigger_id=1,name=str4,value=444 ddd,ts=40000000;"
           "cmd=add,sk=2,attr2=2,trigger_id=1,name=str5,value=555 eee,ts=50000000;"
           "cmd=add,sk=0,attr2=2,trigger_id=2,name=str6,value=666 fff,ts=60000000";
    ttl = std::numeric_limits<int64_t>::max();
}

IndexPartitionPtr KkvTableQueryTest::createKkvPartition(const string tableName)
{
    std::string fields, attributes, docs, pkeyField, skeyField;
    int64_t ttl;
    prepareKkvTableData(fields, pkeyField, skeyField, attributes, docs, ttl);
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/kkv_" + tableName;
    auto schemaPtr =
        IndexlibPartitionCreator::CreateKKVSchema(tableName, fields, pkeyField, skeyField, attributes, true, ttl);
    return IndexlibPartitionCreator::CreateIndexPartition(schemaPtr, indexRoot, docs);
}

IndexPartitionPtr KkvTableQueryTest::createStringKkvPartition(const string tableName)
{
    std::string fields, attributes, docs, pkeyField, skeyField;
    int64_t ttl;
    prepareKkvTableStringData(fields, pkeyField, skeyField, attributes, docs, ttl);
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/kkv_" + tableName;
    auto schemaPtr =
        IndexlibPartitionCreator::CreateKKVSchema(tableName, fields, pkeyField, skeyField, attributes, true, ttl);
    return IndexlibPartitionCreator::CreateIndexPartition(schemaPtr, indexRoot, docs);
}

void KkvTableQueryTest::TestReadKkvTable()
{
    auto kkvPartition = createKkvPartition("kkvtable2");
    auto reader = kkvPartition->GetReader();
    PartitionQuery request;
    request.add_pk("1");
    request.add_skey("1");
    request.add_attrs("attr2");
    request.set_limit(INT64_MAX);
    PartitionResponse response;
    auto status = KkvTableExecutor::QueryKkvTable(reader, DEFAULT_REGIONID, request, response);
    EXPECT_TRUE(status.IsOK());
    EXPECT_EQ(1, response.rows_size());
    EXPECT_EQ(1, response.rows(0).attrvalues_size());
    EXPECT_EQ(ValueType::UINT_32, response.rows(0).attrvalues(0).type()); // attr2
    EXPECT_EQ(2, response.rows(0).attrvalues(0).uint32_value());          // attr2
}

void KkvTableQueryTest::TestReadKkvTableWithEmptySk()
{
    {
        auto kkvPartition = createKkvPartition("kkvtable1");
        auto reader = kkvPartition->GetReader();

        PartitionQuery request;
        request.add_pk("0");
        request.add_attrs("");

        request.set_limit(INT64_MAX);
        PartitionResponse response;
        auto status = KkvTableExecutor::QueryKkvTable(reader, DEFAULT_REGIONID, request, response);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
        string attrInfo = R"(fields {
  attrName: "attr2"
  type: UINT_32
}
fields {
  attrName: "name"
  type: STRING
}
fields {
  attrName: "value"
  type: STRING
}
fields {
  attrName: "attr3"
  type: FLOAT
}
fields {
  attrName: "attr4"
  type: FLOAT
}
fields {
  attrName: "attr5"
  type: INT_16
}
fields {
  attrName: "attr6"
  type: FLOAT
}
fields {
  attrName: "sk"
  type: INT_64
}
)";
        string row0 = R"(attrValues {
  uint32_value: 0
  type: UINT_32
}
attrValues {
  bytes_value: "str1"
  type: STRING
}
attrValues {
  multi_bytes_value {
    value: "111"
    value: "aaa"
  }
  type: STRING
}
attrValues {
  float_value: 1.1
  type: FLOAT
}
attrValues {
  multi_float_value {
    value: 1.1
    value: 1.2
  }
  type: FLOAT
}
attrValues {
  multi_int32_value {
    value: 60
    value: 70
    value: 80
  }
  type: INT_16
}
attrValues {
  multi_float_value {
    value: 6
    value: 7
    value: 8
  }
  type: FLOAT
}
attrValues {
  int64_value: 0
  type: INT_64
}
)";
        EXPECT_EQ(attrInfo, response.attrinfo().DebugString());
        EXPECT_EQ(row0, response.rows(0).DebugString());
    }

    {
        auto kkvPartition = createKkvPartition("kkvtable2");
        auto reader = kkvPartition->GetReader();

        PartitionQuery request;
        request.add_pk("1");
        request.add_attrs("value");
        request.add_attrs("attr3");
        request.add_attrs("attr4");
        request.add_attrs("sk");

        request.set_limit(INT64_MAX);
        PartitionResponse response;
        auto status = KkvTableExecutor::QueryKkvTable(reader, DEFAULT_REGIONID, request, response);

        ASSERT_TRUE(status.IsOK()) << status.ToString();
        EXPECT_EQ(3, response.rows_size());

        EXPECT_EQ(4, response.rows(0).attrvalues_size());
        EXPECT_EQ(ValueType::STRING, response.rows(0).attrvalues(0).type());                     // value
        EXPECT_EQ(ValueType::FLOAT, response.rows(0).attrvalues(1).type());                      // attr3
        EXPECT_EQ(ValueType::FLOAT, response.rows(0).attrvalues(2).type());                      // attr4
        EXPECT_EQ(ValueType::INT_64, response.rows(0).attrvalues(3).type());                     // sk
        EXPECT_EQ(2, response.rows(0).attrvalues(0).multi_bytes_value().value_size());           // value
        EXPECT_EQ("333", response.rows(0).attrvalues(0).multi_bytes_value().value(0));           // value
        EXPECT_EQ("ccc", response.rows(0).attrvalues(0).multi_bytes_value().value(1));           // value
        EXPECT_NEAR(3.1, response.rows(0).attrvalues(1).float_value(), 0.000001);                // attr3
        EXPECT_EQ(2, response.rows(0).attrvalues(2).multi_float_value().value_size());           // attr4
        EXPECT_NEAR(3.1, response.rows(0).attrvalues(2).multi_float_value().value(0), 0.000001); // attr4
        EXPECT_NEAR(3.2, response.rows(0).attrvalues(2).multi_float_value().value(1), 0.000001); // attr4
        EXPECT_EQ(0, response.rows(0).attrvalues(3).int64_value());                              // sk

        EXPECT_EQ(4, response.rows(1).attrvalues_size());
        EXPECT_EQ(ValueType::STRING, response.rows(1).attrvalues(0).type()); // value
        EXPECT_EQ(ValueType::FLOAT, response.rows(1).attrvalues(1).type());  // attr3
        EXPECT_EQ(ValueType::FLOAT, response.rows(1).attrvalues(2).type());  // attr4
        EXPECT_EQ(ValueType::INT_64, response.rows(1).attrvalues(3).type()); // sk

        EXPECT_EQ(2, response.rows(1).attrvalues(0).multi_bytes_value().value_size());           // value
        EXPECT_EQ("444", response.rows(1).attrvalues(0).multi_bytes_value().value(0));           // value
        EXPECT_EQ("ddd", response.rows(1).attrvalues(0).multi_bytes_value().value(1));           // value
        EXPECT_NEAR(4.1, response.rows(1).attrvalues(1).float_value(), 0.000001);                // attr3
        EXPECT_EQ(2, response.rows(1).attrvalues(2).multi_float_value().value_size());           // attr4
        EXPECT_NEAR(4.1, response.rows(1).attrvalues(2).multi_float_value().value(0), 0.000001); // attr4
        EXPECT_NEAR(4.2, response.rows(1).attrvalues(2).multi_float_value().value(1), 0.000001); // attr4
        EXPECT_EQ(1, response.rows(1).attrvalues(3).int64_value());                              // sk

        EXPECT_EQ(4, response.rows(2).attrvalues_size());
        EXPECT_EQ(ValueType::STRING, response.rows(2).attrvalues(0).type()); // value
        EXPECT_EQ(ValueType::FLOAT, response.rows(2).attrvalues(1).type());  // attr3
        EXPECT_EQ(ValueType::FLOAT, response.rows(2).attrvalues(2).type());  // attr4
        EXPECT_EQ(ValueType::INT_64, response.rows(2).attrvalues(3).type()); // sk

        EXPECT_EQ(2, response.rows(2).attrvalues(0).multi_bytes_value().value_size());           // value
        EXPECT_EQ("555", response.rows(2).attrvalues(0).multi_bytes_value().value(0));           // value
        EXPECT_EQ("eee", response.rows(2).attrvalues(0).multi_bytes_value().value(1));           // value
        EXPECT_NEAR(5.1, response.rows(2).attrvalues(1).float_value(), 0.000001);                // attr3
        EXPECT_EQ(2, response.rows(2).attrvalues(2).multi_float_value().value_size());           // attr4
        EXPECT_NEAR(5.1, response.rows(2).attrvalues(2).multi_float_value().value(0), 0.000001); // attr4
        EXPECT_NEAR(5.2, response.rows(2).attrvalues(2).multi_float_value().value(1), 0.000001); // attr4
        EXPECT_EQ(2, response.rows(2).attrvalues(3).int64_value());                              // sk
    }
}

void KkvTableQueryTest::TestQueryWithSkInAttr()
{
    auto kkvPartition = createKkvPartition("kkvtable1");
    auto reader = kkvPartition->GetReader();

    indexlibv2::base::PartitionQuery request;
    request.add_pk("0");
    request.add_attrs("sk");
    indexlibv2::base::PartitionResponse response;
    auto status = KkvTableExecutor::QueryKkvTable(reader, DEFAULT_REGIONID, request, response);
    EXPECT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_EQ(2, response.rows_size());
    EXPECT_EQ(1, response.rows(0).attrvalues_size());
    EXPECT_EQ(ValueType::INT_64, response.rows(0).attrvalues(0).type());
    EXPECT_EQ(0, response.rows(0).attrvalues(0).int64_value());
}

}} // namespace indexlib::tools
