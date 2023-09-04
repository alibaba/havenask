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

class TableServiceQueryIndexTest : public TESTBASE {

private:
    IndexPartitionPtr createIndexTablePartition(string tableName, string pkType);

public:
    void setUp();

private:
    PartitionId _pid1;
    PartitionId _pid2;
    PartitionId _pid3;
    MultiTableReader _multiReader1;
    IndexProvider _provider1;
    std::shared_ptr<TableServiceImpl> tableServiceImpl;
};

IndexPartitionPtr TableServiceQueryIndexTest::createIndexTablePartition(string tableName, string pkType) {
    string indexRoot = GET_TEMPLATE_DATA_PATH() + "/index_" + tableName;
    auto schemaPtr = IndexlibPartitionCreator::CreateSchema(
        tableName,
        "key:string;key1:uint32;key2:text;attr1:float;attr2:int32;"
        "attr3:float:true;attr4:int32:true;attr5:string;attr6:string:true;attr7:double;attr8:int16:true:true::3;attr9:"
        "float:true:true::3;",
        "pk:" + pkType + ":key;index:number:key1;strIndex:text:key2",
        "pack1:attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9;",
        "");
    string docStr = "cmd=add,key=123456,key1=12,key2=abc,"
                    "attr1=2.2,"
                    "attr2=3,"
                    "attr3=11.1 12.1,"
                    "attr4=22 23 24,"
                    "attr5=7.7,"
                    "attr6=8.1 8.2 8.3 8.4,"
                    "attr7=7.23,"
                    "attr8=61 62 63,"
                    "attr9=6.1 6.2 6.3;"
                    "cmd=add,key=456789,key1=23,key2=bcd,"
                    "attr1=4.4,"
                    "attr2=5,"
                    "attr3=41.1 42.1,"
                    "attr4=52 53 54,"
                    "attr5=9.9,"
                    "attr6=10.1 10.2 10.3 10.4,"
                    "attr7=8.67,"
                    "attr8=71 72 73,"
                    "attr9=7.1 7.2 7.3;"
                    "cmd=add,key=987654,key1=34,key2=cde,"
                    "attr1=8.8,"
                    "attr2=7,"
                    "attr3=51.1 52.1,"
                    "attr4=62 63 64,"
                    "attr5=7.7,"
                    "attr6=20.1 20.2 20.3 20.4,"
                    "attr7=5.67,"
                    "attr8=81 82 83,"
                    "attr9=9.2 9.3 9.4;"
                    "cmd=delete,key=987654;";
    const IndexPartitionOptions options = IndexPartitionOptions();
    const std::string indexPluginPath = "";
    bool needMerge = false;
    return IndexlibPartitionCreator::CreateIndexPartition(
        schemaPtr, indexRoot, docStr, options, indexPluginPath, needMerge);
}

void TableServiceQueryIndexTest::setUp() {
    _pid1.tableId.fullVersion = 1;
    _pid1.tableId.tableName = "table1";
    _pid1.from = 0;
    _pid1.to = 65535;
    _pid2.tableId.fullVersion = 2;
    _pid2.tableId.tableName = "table2";
    _pid2.from = 0;
    _pid2.to = 32767;
    _pid3.tableId.fullVersion = 2;
    _pid3.tableId.tableName = "table2";
    _pid3.from = 32768;
    _pid3.to = 65535;
    auto indexPartition_1 = createIndexTablePartition("table1", "primarykey64");
    auto indexPartition_2 = createIndexTablePartition("table2", "primarykey128");
    auto indexPartition_3 = createIndexTablePartition("table2", "primarykey128");
    auto data1 = std::make_shared<SuezIndexPartitionData>(_pid1, 1, indexPartition_1, true);
    auto data2 = std::make_shared<SuezIndexPartitionData>(_pid2, 1, indexPartition_2, true);
    auto data3 = std::make_shared<SuezIndexPartitionData>(_pid3, 1, indexPartition_3, true);
    SingleTableReaderPtr singleReader1(new SingleTableReader(data1));
    SingleTableReaderPtr singleReader2(new SingleTableReader(data2));
    SingleTableReaderPtr singleReader3(new SingleTableReader(data3));
    _multiReader1.addTableReader(_pid1, singleReader1);
    _multiReader1.addTableReader(_pid2, singleReader2);
    _multiReader1.addTableReader(_pid3, singleReader3);
    _provider1.multiTableReader = _multiReader1;
    _provider1.tableReader[_pid1] = indexPartition_1;
    _provider1.tableReader[_pid2] = indexPartition_2;
    _provider1.tableReader[_pid3] = indexPartition_3;
    tableServiceImpl = make_shared<TableServiceImpl>();
    tableServiceImpl->setIndexProvider(make_shared<IndexProvider>(_provider1));
}

TEST_F(TableServiceQueryIndexTest, queryIndexTable) {

    TableQueryRequest request;
    request.set_table("table1");
    request.set_pk("123456");
    request.set_attr("attr2");
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
    EXPECT_EQ(1, result.docvalueset_size());
    EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
    EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).intvalue());
    EXPECT_EQ(0, result.docvalueset(0).docid());
}

TEST_F(TableServiceQueryIndexTest, testReadAttrWithPk) {

    {
        // pk 64
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("123456");
        request.set_attr("attr2");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).intvalue());
        EXPECT_EQ(0, result.docvalueset(0).docid());
    }

    {
        // pk128
        TableQueryRequest request;
        request.set_table("table2");
        request.set_pk("456789");
        request.set_attr("");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).docid());
        EXPECT_EQ(9, result.docvalueset(0).attrvalue().size());
        EXPECT_NEAR(4.4, result.docvalueset(0).attrvalue(0).doublevalue(), 0.000001);
        EXPECT_EQ(5, result.docvalueset(0).attrvalue(1).intvalue());
        EXPECT_NEAR(41.1, result.docvalueset(0).attrvalue(2).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(42.1, result.docvalueset(0).attrvalue(2).multidoublevalue(1), 0.00001);
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(3).multiintvalue().size());
        EXPECT_EQ(52, result.docvalueset(0).attrvalue(3).multiintvalue(0));
        EXPECT_EQ(53, result.docvalueset(0).attrvalue(3).multiintvalue(1));
        EXPECT_EQ(54, result.docvalueset(0).attrvalue(3).multiintvalue(2));
        EXPECT_EQ("9.9", result.docvalueset(0).attrvalue(4).strvalue());
        EXPECT_EQ(4, result.docvalueset(0).attrvalue(5).multistrvalue().size());
        EXPECT_EQ("10.1", result.docvalueset(0).attrvalue(5).multistrvalue(0));
        EXPECT_EQ("10.2", result.docvalueset(0).attrvalue(5).multistrvalue(1));
        EXPECT_EQ("10.3", result.docvalueset(0).attrvalue(5).multistrvalue(2));
        EXPECT_EQ("10.4", result.docvalueset(0).attrvalue(5).multistrvalue(3));
        EXPECT_NEAR(8.67, result.docvalueset(0).attrvalue(6).doublevalue(), 0.00001);
        EXPECT_EQ(71, result.docvalueset(0).attrvalue(7).multiintvalue(0));
        EXPECT_EQ(72, result.docvalueset(0).attrvalue(7).multiintvalue(1));
        EXPECT_EQ(73, result.docvalueset(0).attrvalue(7).multiintvalue(2));
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(8).multidoublevalue().size());
        EXPECT_NEAR(7.1, result.docvalueset(0).attrvalue(8).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(7.2, result.docvalueset(0).attrvalue(8).multidoublevalue(1), 0.00001);
        EXPECT_NEAR(7.3, result.docvalueset(0).attrvalue(8).multidoublevalue(2), 0.00001);
    }
}

TEST_F(TableServiceQueryIndexTest, testQueryWithInvalidPk) {
    TableQueryRequest request;
    request.set_table("table1");
    request.set_pk("invalid_pk");
    request.set_attr("");
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NO_RECORD, errorInfo.errorcode());
    EXPECT_EQ(0, result.docvalueset_size());
}

TEST_F(TableServiceQueryIndexTest, testQueryWithInvalidAttr) {
    TableQueryRequest request;
    request.set_table("table1");
    request.set_pk("123456");
    request.set_attr("invalid_attr");
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
    EXPECT_EQ(1, result.docvalueset_size());
}

TEST_F(TableServiceQueryIndexTest, testQueryDeletedPk) {
    TableQueryRequest request;
    request.set_table("table1");
    request.set_pk("987654");
    request.set_attr("");
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NO_RECORD, errorInfo.errorcode());
}

TEST_F(TableServiceQueryIndexTest, testReadDocIdWithPk) {
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("123456");
        request.set_querydoc(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docids_size());
        EXPECT_EQ(0, result.docids(0));
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_pk("12345678");
        request.set_querydoc(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NO_RECORD, errorInfo.errorcode());
    }
}

TEST_F(TableServiceQueryIndexTest, testReadAttrWithIndex) {

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index");
        request.set_indexvalue("23");
        request.set_attr("attr2");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).docid());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(5, result.docvalueset(0).attrvalue(0).intvalue());
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index");
        request.set_indexvalue("23");
        request.set_attr("attr8");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).docid());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).multiintvalue().size());
        EXPECT_EQ(71, result.docvalueset(0).attrvalue(0).multiintvalue(0));
        EXPECT_EQ(72, result.docvalueset(0).attrvalue(0).multiintvalue(1));
        EXPECT_EQ(73, result.docvalueset(0).attrvalue(0).multiintvalue(2));
    }
    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index");
        request.set_indexvalue("23");
        request.set_attr("attr9");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).docid());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).multidoublevalue().size());
        EXPECT_NEAR(7.1, result.docvalueset(0).attrvalue(0).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(7.2, result.docvalueset(0).attrvalue(0).multidoublevalue(1), 0.00001);
        EXPECT_NEAR(7.3, result.docvalueset(0).attrvalue(0).multidoublevalue(2), 0.00001);
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index_index");
        request.set_indexvalue("23");
        request.set_attr("attr9");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_CREATE_READER, errorInfo.errorcode());
    }
    {
        // test text index
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("strIndex");
        request.set_indexvalue("bcd");
        request.set_attr("attr9");
        request.set_limit(INT64_MAX);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).docid());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).multidoublevalue().size());
        EXPECT_NEAR(7.1, result.docvalueset(0).attrvalue(0).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(7.2, result.docvalueset(0).attrvalue(0).multidoublevalue(1), 0.00001);
        EXPECT_NEAR(7.3, result.docvalueset(0).attrvalue(0).multidoublevalue(2), 0.00001);
    }

    {
        // test deleted index
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index");
        request.set_indexvalue("34");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NO_RECORD, errorInfo.errorcode());
    }

    {
        // test ignore
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index");
        request.set_indexvalue("34");
        request.set_ignoredeletionmap(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docids_size());
    }
}

TEST_F(TableServiceQueryIndexTest, testReadDocIdWithIndex) {

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index");
        request.set_indexvalue("23");
        request.set_querydoc(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docids_size());
        EXPECT_EQ(1, result.docids(0));
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_indexname("index_index");
        request.set_indexvalue("23");
        request.set_querydoc(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_CREATE_READER, errorInfo.errorcode());
    }
}

TEST_F(TableServiceQueryIndexTest, testReadAttrWithDocId) {

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_docid(0);
        request.set_attr("attr2");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).intvalue());
    }

    {
        TableQueryRequest request;
        request.set_table("table1");
        request.set_docid(1);
        request.set_attr("attr9");
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ(3, result.docvalueset(0).attrvalue(0).multidoublevalue().size());
        EXPECT_NEAR(7.1, result.docvalueset(0).attrvalue(0).multidoublevalue(0), 0.00001);
        EXPECT_NEAR(7.2, result.docvalueset(0).attrvalue(0).multidoublevalue(1), 0.00001);
        EXPECT_NEAR(7.3, result.docvalueset(0).attrvalue(0).multidoublevalue(2), 0.00001);
    }
}

TEST_F(TableServiceQueryIndexTest, testEncodeStrValue) {

    {
        TableQueryRequest request;
        request.set_table("table2");
        request.set_pk("456789");
        request.set_attr("attr5");
        request.set_encodestr(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("OS45", result.docvalueset(0).attrvalue(0).strvalue());
    }
    {
        TableQueryRequest request;
        request.set_table("table2");
        request.set_pk("456789");
        request.set_attr("attr6");
        request.set_encodestr(true);
        TableQueryResponse response;
        tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
        auto result = response.res();
        auto errorInfo = response.errorinfo();
        EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
        EXPECT_EQ(1, result.docvalueset_size());
        EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
        EXPECT_EQ("MTAuMQ==", result.docvalueset(0).attrvalue(0).multistrvalue(0));
        EXPECT_EQ("MTAuMg==", result.docvalueset(0).attrvalue(0).multistrvalue(1));
        EXPECT_EQ("MTAuMw==", result.docvalueset(0).attrvalue(0).multistrvalue(2));
        EXPECT_EQ("MTAuNA==", result.docvalueset(0).attrvalue(0).multistrvalue(3));
    }
}

TEST_F(TableServiceQueryIndexTest, testPartitions) {
    TableQueryRequest request;
    request.set_table("table2");
    request.set_pk("456789");
    request.set_attr("attr5");
    auto partitions = request.mutable_partition();
    partitions->Add(0);
    partitions->Add(32767);
    request.set_encodestr(true);
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NONE, errorInfo.errorcode());
    EXPECT_EQ(1, result.docvalueset_size());
    EXPECT_EQ(1, result.docvalueset(0).attrvalue().size());
    EXPECT_EQ("OS45", result.docvalueset(0).attrvalue(0).strvalue());
}

TEST_F(TableServiceQueryIndexTest, testInvalidPartition) {
    TableQueryRequest request;
    request.set_table("table1");
    request.set_pk("123456");
    request.set_attr("attr2");
    auto partitions = request.mutable_partition();
    partitions->Add(0);
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NO_TABLE, errorInfo.errorcode());
}

TEST_F(TableServiceQueryIndexTest, testNotExistPartition) {
    TableQueryRequest request;
    request.set_table("table1");
    request.set_pk("123456");
    request.set_attr("attr2");
    auto partitions = request.mutable_partition();
    partitions->Add(0);
    partitions->Add(32767);
    TableQueryResponse response;
    tableServiceImpl->queryTable(nullptr, &request, &response, nullptr);
    auto result = response.res();
    auto errorInfo = response.errorinfo();
    EXPECT_EQ(TBS_ERROR_NO_TABLE, errorInfo.errorcode());
}

} // namespace suez
