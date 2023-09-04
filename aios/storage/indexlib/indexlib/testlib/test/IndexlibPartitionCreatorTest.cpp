#include "indexlib/config/SortDescription.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/test/unittest.h"
#include "indexlib/testlib/indexlib_partition_creator.h"

using namespace std;

namespace indexlib { namespace testlib {

class IndexlibPartitionCreatorTest : public INDEXLIB_TESTBASE
{
public:
    IndexlibPartitionCreatorTest();
    ~IndexlibPartitionCreatorTest();

    DECLARE_CLASS_NAME(IndexlibPartitionCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSortDescription();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexlibPartitionCreatorTest, TestSortDescription);
AUTIL_LOG_SETUP(indexlib.testlib, IndexlibPartitionCreatorTest);

IndexlibPartitionCreatorTest::IndexlibPartitionCreatorTest() {}

IndexlibPartitionCreatorTest::~IndexlibPartitionCreatorTest() {}

void IndexlibPartitionCreatorTest::CaseSetUp() {}

void IndexlibPartitionCreatorTest::CaseTearDown() {}

void IndexlibPartitionCreatorTest::TestSortDescription()
{
    string field = "name:string:false:false:uniq;price:float;category:int32";
    string index = "pk:primarykey64:name;categoryIndex:number:category";
    string attributes = "name;price;category";
    config::IndexPartitionSchemaPtr schema =
        IndexlibPartitionCreator::CreateSchema("index_table", field, index, attributes, "");

    indexlibv2::config::SortDescriptions sortDescriptions {
        indexlibv2::config::SortDescription("category", indexlibv2::config::sp_desc),
        indexlibv2::config::SortDescription("price", indexlibv2::config::sp_asc)};

    config::IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;

    string unorderedDocs = "cmd=add,name=doc1,price=10,category=1;"
                           "cmd=add,name=doc2,price=20,category=2;"
                           "cmd=add,name=doc3,price=30,category=3;"
                           "cmd=add,name=doc4,price=40,category=4;"
                           "cmd=update_field,name=doc1,price=11;";
    ASSERT_THROW(IndexlibPartitionCreator::CreateIndexPartition(schema, GET_TEMP_DATA_PATH(), unorderedDocs, options,
                                                                "", false, sortDescriptions),
                 util::ExceptionBase);

    string orderedDocs = "cmd=add,name=doc1,price=10,category=4;"
                         "cmd=add,name=doc2,price=20,category=3;"
                         "cmd=add,name=doc3,price=30,category=2;"
                         "cmd=add,name=doc4,price=40,category=1;"
                         "cmd=update_field,name=doc1,price=11;";

    partition::IndexPartitionPtr partition = IndexlibPartitionCreator::CreateIndexPartition(
        schema, GET_TEMP_DATA_PATH(), orderedDocs, options, "", false, sortDescriptions);
    index::PartitionInfoPtr partitionInfo = partition->GetPartitionData()->GetPartitionInfo();
    DocIdRangeVector ranges;
    ASSERT_TRUE(partitionInfo->GetOrderedDocIdRanges(ranges));
}

}} // namespace indexlib::testlib
