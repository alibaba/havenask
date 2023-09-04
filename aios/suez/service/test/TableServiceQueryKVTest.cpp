#include "indexlib/testlib/indexlib_partition_creator.h"
#include "suez/service/TableServiceImpl.h"
#include "suez/table/test/MockSuezPartition.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using indexlib::config::IndexPartitionOptions;
using indexlib::partition::IndexPartitionPtr;
using indexlib::testlib::IndexlibPartitionCreator;

namespace suez {
class TableServiceQueryKVTest : public TESTBASE {

private:
    IndexPartitionPtr createKVPartition(string tableName);

public:
    void setUp();

private:
    PartitionId _pid1;
    MultiTableReader _multiReader1;
    IndexProvider _provider1;
    std::shared_ptr<TableServiceImpl> tableServiceImpl;
};

IndexPartitionPtr TableServiceQueryKVTest::createKVPartition(string tableName) {
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/kv_" + tableName;
    auto schemaPtr = IndexlibPartitionCreator::CreateKVSchema(
        tableName,
        "key:uint64;attr1:int32;attr2:float;attr3:int32:true;attr4:float:true;attr5:string;attr6:string:true;"
        "attr7:int16:true:true::5;attr8:float:true:true::6",
        "key",
        "attr1;attr2;attr3;attr4;attr5;attr6;attr7;attr8");
    string docStr = "cmd=add,key=1,"
                    "attr1=2,"
                    "attr2=3.3,"
                    "attr3=11 12,"
                    "attr4=22.1 22.2 22.3,"
                    "attr5=7.7,"
                    "attr6=8.1 8.2 8.3 8.4,"
                    "attr7=9 10 9 10 8,"
                    "attr8=9.1 9.2 9.3 9.4 9.5 9.6;"
                    "cmd=add,key=2,"
                    "attr1=4,"
                    "attr2=5.5,"
                    "attr3=41 42,"
                    "attr4=52.1 52.2 52.3,"
                    "attr5=9.9,"
                    "attr6=10.1 10.2 10.3 10.4,"
                    "attr7=29 20 29 20 28,"
                    "attr8=29.1 29.2 29.3 29.4 29.5 29.6;";
    return IndexlibPartitionCreator::CreateIndexPartition(schemaPtr, indexRoot, docStr);
}

void TableServiceQueryKVTest::setUp() {
    _pid1.tableId.fullVersion = 1;
    _pid1.tableId.tableName = "table1";
    _pid1.from = 0;
    _pid1.to = 65535;
    auto partitionPtr = createKVPartition("table1");
    auto data1 = std::make_shared<SuezIndexPartitionData>(_pid1, 1, partitionPtr, true);
    SingleTableReaderPtr singleReader1(new SingleTableReader(data1));
    _multiReader1.addTableReader(_pid1, singleReader1);
    _provider1.multiTableReader = _multiReader1;
    tableServiceImpl = make_shared<TableServiceImpl>();
    tableServiceImpl->setIndexProvider(make_shared<IndexProvider>(_provider1));
}

TEST_F(TableServiceQueryKVTest, testReadKVTable) {

    // query int
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_attr("attr1");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).intvalue());
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pknumber(1);
        request.set_attr("attr1");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).intvalue());
    }

    // query float
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_attr("attr2");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_NEAR(3.3, result.docvalueset(0).attrvalue(0).doublevalue(), 0.00001);
    }

    // query string
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_attr("attr5");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("7.7", result.docvalueset(0).attrvalue(0).strvalue());
    }

    // query multi int
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("2");
        request.set_attr("attr3");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).multiintvalue().size());
        EXPECT_EQ(41, result.docvalueset(0).attrvalue(0).multiintvalue(0));
        EXPECT_EQ(42, result.docvalueset(0).attrvalue(0).multiintvalue(1));
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pknumber(2);
        request.set_attr("attr3");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(2, result.docvalueset(0).attrvalue(0).multiintvalue().size());
        EXPECT_EQ(41, result.docvalueset(0).attrvalue(0).multiintvalue(0));
        EXPECT_EQ(42, result.docvalueset(0).attrvalue(0).multiintvalue(1));
    }

    // query multi float
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("2");
        request.set_attr("attr4");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).multidoublevalue().size());
        EXPECT_NEAR(52.1, result.docvalueset(0).attrvalue(0).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(52.2, result.docvalueset(0).attrvalue(0).multidoublevalue(1), 0.00001);
        EXPECT_NEAR(52.3, result.docvalueset(0).attrvalue(0).multidoublevalue(2), 0.00001);
    }

    // query multi string
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("2");
        request.set_attr("attr6");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(4, result.docvalueset(0).attrvalue(0).multistrvalue().size());
        EXPECT_EQ("10.1", result.docvalueset(0).attrvalue(0).multistrvalue(0));
        EXPECT_EQ("10.2", result.docvalueset(0).attrvalue(0).multistrvalue(1));
        EXPECT_EQ("10.3", result.docvalueset(0).attrvalue(0).multistrvalue(2));
        EXPECT_EQ("10.4", result.docvalueset(0).attrvalue(0).multistrvalue(3));
    }

    // query counted multi int16
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("2");
        request.set_attr("attr7");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(5, result.docvalueset(0).attrvalue(0).multiintvalue().size());
        EXPECT_EQ(29, result.docvalueset(0).attrvalue(0).multiintvalue(0));
        EXPECT_EQ(20, result.docvalueset(0).attrvalue(0).multiintvalue(1));
        EXPECT_EQ(29, result.docvalueset(0).attrvalue(0).multiintvalue(2));
        EXPECT_EQ(20, result.docvalueset(0).attrvalue(0).multiintvalue(3));
        EXPECT_EQ(28, result.docvalueset(0).attrvalue(0).multiintvalue(4));
    }

    // query conted multi float
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("2");
        request.set_attr("attr8");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(6, result.docvalueset(0).attrvalue(0).multidoublevalue().size());
        EXPECT_NEAR(29.1, result.docvalueset(0).attrvalue(0).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(29.2, result.docvalueset(0).attrvalue(0).multidoublevalue(1), 0.00001);
        EXPECT_NEAR(29.3, result.docvalueset(0).attrvalue(0).multidoublevalue(2), 0.00001);
        EXPECT_NEAR(29.4, result.docvalueset(0).attrvalue(0).multidoublevalue(3), 0.00001);
        EXPECT_NEAR(29.5, result.docvalueset(0).attrvalue(0).multidoublevalue(4), 0.00001);
        EXPECT_NEAR(29.6, result.docvalueset(0).attrvalue(0).multidoublevalue(5), 0.00001);
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("3");
        request.set_attr("attr6");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NO_RECORD, errorInfo.errorcode());
        EXPECT_EQ(0, result.docvalueset_size());
    }
}

TEST_F(TableServiceQueryKVTest, testEncodeStr) {
    auto kvPartition = createKVPartition("table1");
    {
        // string 7.7
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("1");
        request.set_attr("attr5");
        request.set_encodestr(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("Ny43", result.docvalueset(0).attrvalue(0).strvalue());
    }

    {
        // multi string
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("2");
        request.set_attr("attr6");
        request.set_encodestr(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(4, result.docvalueset(0).attrvalue(0).multistrvalue().size());
        EXPECT_EQ("MTAuMQ==", result.docvalueset(0).attrvalue(0).multistrvalue(0));
        EXPECT_EQ("MTAuMg==", result.docvalueset(0).attrvalue(0).multistrvalue(1));
        EXPECT_EQ("MTAuMw==", result.docvalueset(0).attrvalue(0).multistrvalue(2));
        EXPECT_EQ("MTAuNA==", result.docvalueset(0).attrvalue(0).multistrvalue(3));
    }
}

} // namespace suez
