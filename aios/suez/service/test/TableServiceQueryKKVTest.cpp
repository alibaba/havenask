#include "indexlib/testlib/indexlib_partition_creator.h"
#include "suez/service/TableServiceImpl.h"
#include "suez/table/test/MockSuezPartition.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using indexlib::partition::IndexPartitionPtr;
using indexlib::testlib::IndexlibPartitionCreator;

namespace suez {
class TableServiceQueryKKVTest : public TESTBASE {

private:
    void prepareKkvTableData(std::string &fields,
                             std::string &pkeyField,
                             std::string &skeyField,
                             std::string &attributes,
                             std::string &docs,
                             int64_t &ttl);

    void prepareKkvTableStringData(std::string &fields,
                                   std::string &pkeyField,
                                   std::string &skeyField,
                                   std::string &attributes,
                                   std::string &docs,
                                   int64_t &ttl);

    IndexPartitionPtr createKKVPartition(string tableName);
    IndexPartitionPtr createStringKKVPartition(string tableName);

public:
    void setUp();

private:
    PartitionId _pid1;
    PartitionId _pid2;
    MultiTableReader _multiReader1;
    IndexProvider _provider1;
    std::shared_ptr<TableServiceImpl> tableServiceImpl;
};

void TableServiceQueryKKVTest::prepareKkvTableData(std::string &fields,
                                                   std::string &pkeyField,
                                                   std::string &skeyField,
                                                   std::string &attributes,
                                                   std::string &docs,
                                                   int64_t &ttl) {
    fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true;attr3:float;attr4:float:true;attr5:"
             "int16:"
             "true:true::3;attr6:float:true:true::3";
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

void TableServiceQueryKKVTest::prepareKkvTableStringData(std::string &fields,
                                                         std::string &pkeyField,
                                                         std::string &skeyField,
                                                         std::string &attributes,
                                                         std::string &docs,
                                                         int64_t &ttl) {
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

IndexPartitionPtr TableServiceQueryKKVTest::createKKVPartition(string tableName) {
    std::string fields, attributes, docs, pkeyField, skeyField;
    int64_t ttl;
    prepareKkvTableData(fields, pkeyField, skeyField, attributes, docs, ttl);
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/kkv_" + tableName;
    auto schemaPtr = IndexlibPartitionCreator::CreateKKVSchema(tableName, fields, pkeyField, skeyField, attributes);
    return IndexlibPartitionCreator::CreateIndexPartition(schemaPtr, indexRoot, docs);
}

IndexPartitionPtr TableServiceQueryKKVTest::createStringKKVPartition(string tableName) {
    std::string fields, attributes, docs, pkeyField, skeyField;
    int64_t ttl;
    prepareKkvTableStringData(fields, pkeyField, skeyField, attributes, docs, ttl);
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/kkv_" + tableName;
    auto schemaPtr = IndexlibPartitionCreator::CreateKKVSchema(tableName, fields, pkeyField, skeyField, attributes);
    return IndexlibPartitionCreator::CreateIndexPartition(schemaPtr, indexRoot, docs);
}

void TableServiceQueryKKVTest::setUp() {
    _pid1.tableId.fullVersion = 1;
    _pid1.tableId.tableName = "table1";
    _pid1.from = 0;
    _pid1.to = 65535;
    _pid2.tableId.fullVersion = 2;
    _pid2.tableId.tableName = "table2";
    _pid2.from = 0;
    _pid2.to = 65535;
    auto indexPartition_1 = createKKVPartition("kkvtable1");
    auto indexPartition_2 = createStringKKVPartition("kkvtable2");
    auto data1 = std::make_shared<SuezIndexPartitionData>(_pid1, 1, indexPartition_1, true);
    auto data2 = std::make_shared<SuezIndexPartitionData>(_pid2, 1, indexPartition_2, true);
    SingleTableReaderPtr singleReader1(new SingleTableReader(data1));
    SingleTableReaderPtr singleReader2(new SingleTableReader(data2));
    _multiReader1.addTableReader(_pid1, singleReader1);
    _multiReader1.addTableReader(_pid2, singleReader2);
    _provider1.multiTableReader = _multiReader1;
    _provider1.tableReader[_pid1] = indexPartition_1;
    _provider1.tableReader[_pid2] = indexPartition_2;
    tableServiceImpl = make_shared<TableServiceImpl>();
    tableServiceImpl->setIndexProvider(make_shared<IndexProvider>(_provider1));
}

TEST_F(TableServiceQueryKKVTest, testReadKKVTable) {
    {
        // test int value
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("attr2");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).intvalue()); // attr2
    }

    {
        // test string value
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("name");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("str3", result.docvalueset(0).attrvalue(0).strvalue()); // name
    }

    {
        // test float value
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("attr3");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_NEAR(3.1, result.docvalueset(0).attrvalue(0).doublevalue(), 0.00001); // attr3
    }

    {
        // test multi string
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("value");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).multistrvalue().size());
        EXPECT_EQ("333", result.docvalueset(0).attrvalue(0).multistrvalue(0)); // value
        EXPECT_EQ("ccc", result.docvalueset(0).attrvalue(0).multistrvalue(1)); // value
    }

    {
        // test multi float
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("attr4");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).multidoublevalue().size());
        EXPECT_NEAR(3.1, result.docvalueset(0).attrvalue(0).multidoublevalue(0), 0.00001); // attr4
        EXPECT_NEAR(3.2, result.docvalueset(0).attrvalue(0).multidoublevalue(1), 0.00001); // attr4
    }

    {
        // test counted multi int16
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("attr5");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).multiintvalue().size());
        EXPECT_EQ(62, result.docvalueset(0).attrvalue(0).multiintvalue(0)); // attr5
        EXPECT_EQ(72, result.docvalueset(0).attrvalue(0).multiintvalue(1)); // attr5
        EXPECT_EQ(82, result.docvalueset(0).attrvalue(0).multiintvalue(2)); // attr5
        EXPECT_EQ(63, result.docvalueset(1).attrvalue(0).multiintvalue(0)); // attr5
        EXPECT_EQ(73, result.docvalueset(1).attrvalue(0).multiintvalue(1)); // attr5
        EXPECT_EQ(83, result.docvalueset(1).attrvalue(0).multiintvalue(2)); // attr5
        EXPECT_EQ(64, result.docvalueset(2).attrvalue(0).multiintvalue(0)); // attr5
        EXPECT_EQ(74, result.docvalueset(2).attrvalue(0).multiintvalue(1)); // attr5
        EXPECT_EQ(84, result.docvalueset(2).attrvalue(0).multiintvalue(2)); // attr5
    }

    {
        // query attr2 with sk = 2
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("2");
        request.set_attr("attr2");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).intvalue()); // attr2
    }

    {
        // test limit
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("name");
        request.set_limit(1);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("str3", result.docvalueset(0).attrvalue(0).strvalue()); // name
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("invalid_attr");
        request.set_limit(1);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        ASSERT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(8, result.docvalueset(0).attrvalue().size());
    }
}

TEST_F(TableServiceQueryKKVTest, testReadStringKKVTable) {
    // test pk and sk with string type
    TableQueryRequest request;
    request.set_table("table2");
    request.set_pk("1");
    request.set_sk("2");
    request.set_attr("value");
    request.set_limit(INT64_MAX);
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
    EXPECT_EQ(1, result.docvalueset_size());
    ASSERT_EQ(1, result.docvalueset(0).attrvalue().size());
    ASSERT_EQ(2, result.docvalueset(0).attrvalue(0).multistrvalue().size());
    EXPECT_EQ("555", result.docvalueset(0).attrvalue(0).multistrvalue(0)); // value
    EXPECT_EQ("eee", result.docvalueset(0).attrvalue(0).multistrvalue(1)); // value
}

TEST_F(TableServiceQueryKKVTest, testEncodeStrValue) {
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("name");
        request.set_encodestr(true);
        request.set_limit(INT64_MAX);

        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        ASSERT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("c3RyMw==", result.docvalueset(0).attrvalue(0).strvalue()); // name
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_sk("");
        request.set_attr("value");
        request.set_encodestr(true);
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(3, result.docvalueset_size());
        ASSERT_EQ(1, result.docvalueset(0).attrvalue().size());
        ASSERT_EQ(2, result.docvalueset(0).attrvalue(0).multistrvalue().size());
        EXPECT_EQ("MzMz", result.docvalueset(0).attrvalue(0).multistrvalue(0)); // value
        EXPECT_EQ("Y2Nj", result.docvalueset(0).attrvalue(0).multistrvalue(1)); // value
    }
}

} // namespace suez
