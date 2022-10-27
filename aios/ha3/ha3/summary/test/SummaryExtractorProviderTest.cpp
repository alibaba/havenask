#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractorProvider.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/partition/index_application.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/Request.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);

IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(partition);

BEGIN_HA3_NAMESPACE(summary);

class SummaryExtractorProviderTest : public TESTBASE {
public:
    SummaryExtractorProviderTest();
    ~SummaryExtractorProviderTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool *_pool;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(summary, SummaryExtractorProviderTest);


SummaryExtractorProviderTest::SummaryExtractorProviderTest() {
    _pool = new autil::mem_pool::Pool(1024);
}

SummaryExtractorProviderTest::~SummaryExtractorProviderTest() {
    delete _pool;
}

void SummaryExtractorProviderTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void SummaryExtractorProviderTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SummaryExtractorProviderTest, testFillAttributeToSummary) {
    HA3_LOG(DEBUG, "Begin Test!");

    static const string tableName = "mainTable";
    static const string indexRootPath = __FILE__;

    matchdoc::MatchDocAllocatorPtr mdAllocatorPtr(new matchdoc::MatchDocAllocator(_pool));
    auto schema = IndexlibPartitionCreator::CreateSchema(tableName,
            "pk:string;attr_int32:int32;attr_int64:int64;attr_float:float;multi_int64:int64:true", // fields
            "pk:primarykey64:pk", //indexs
            "attr_int32;attr_int64;attr_float;multi_int64", //attributes
            "", // summarys
            ""); // truncateProfile
    string docStrs = "cmd=add,pk=pk0,attr_int32=-8,attr_int64=16,attr_float=1.0,multi_int64=1,2,3;"
                     "cmd=add,pk=pk1,attr_int32=2,attr_int64=2,attr_float=-2.0,multi_int64=1,1,3;";
    auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            schema, indexRootPath + tableName, docStrs);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    IndexApplication::IndexPartitionMap indexPartitionMap;
    indexPartitionMap.insert(make_pair(schemaName, indexPartition));

    IndexApplicationPtr indexApp(new IndexApplication);
    indexApp->Init(indexPartitionMap, JoinRelationMap());
    PartitionReaderSnapshotPtr _snapshotPtr = indexApp->CreateSnapshot();

    VirtualAttributes virtualAttributes;
    auto tableInfoPtr = TableInfoConfigurator::createFromIndexApp(tableName, indexApp);

    AttributeExpressionCreatorPtr creator(new AttributeExpressionCreator(_pool,
                    mdAllocatorPtr.get(), "mainTable", _snapshotPtr.get(), tableInfoPtr,
                    virtualAttributes, nullptr, CavaPluginManagerPtr(), nullptr));

    HitSummarySchema hitSummarySchema(tableInfoPtr.get());
    ASSERT_EQ(size_t(0), hitSummarySchema.getFieldCount());
    Request *request = new Request();
    ConfigClause *requestConfig = new ConfigClause;
    requestConfig->addKVPair("test","test");
    request->setConfigClause(requestConfig);
    SearchCommonResource resource(_pool, tableInfoPtr, nullptr, nullptr, nullptr,
                                  nullptr, CavaPluginManagerPtr(), request, nullptr, _cavaJitModules);
    SummaryExtractorProvider provider(NULL, NULL, request,
            creator.get(), &hitSummarySchema, resource);

    ASSERT_TRUE(provider.fillAttributeToSummary("attr_int32"));
    const map<summaryfieldid_t, AttributeExpression*>&
        attributesMap = provider.getFilledAttributes();
    ASSERT_EQ(size_t(1), attributesMap.size());

    ASSERT_EQ(size_t(1), hitSummarySchema.getFieldCount());
    const SummaryFieldInfo *fieldInfo =
        hitSummarySchema.getSummaryFieldInfo("attr_int32");
    ASSERT_TRUE(fieldInfo);
    ASSERT_EQ(false, fieldInfo->isMultiValue);
    ASSERT_EQ(ft_int32, fieldInfo->fieldType);

    ASSERT_TRUE(provider.fillAttributeToSummary("attr_int32"));
    ASSERT_TRUE(!provider.fillAttributeToSummary("notExistField"));

    ASSERT_TRUE(provider.fillAttributeToSummary("multi_int64"));
    const map<summaryfieldid_t, AttributeExpression*>&
        attributesMap2 = provider.getFilledAttributes();
    ASSERT_EQ(size_t(2), attributesMap2.size());

    ASSERT_EQ(size_t(2), hitSummarySchema.getFieldCount());
    fieldInfo = hitSummarySchema.getSummaryFieldInfo("multi_int64");
    ASSERT_TRUE(fieldInfo);
    ASSERT_EQ(true, fieldInfo->isMultiValue);
    ASSERT_EQ(ft_int64, fieldInfo->fieldType);

    delete request;
}

END_HA3_NAMESPACE(summary);
