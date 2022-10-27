#pragma once
#include <unittest/unittest.h>
#define private public
#define protected public

#include <autil/StringUtil.h>
#include <autil/MultiValueCreator.h>
#include <autil/MultiValueType.h>
#include <autil/legacy/any.h>
#include <suez/turing/common/SessionResource.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <suez/search/ServiceInfo.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <matchdoc/MatchDocAllocator.h>
#include <navi/engine/KernelTesterBuilder.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/tvf/TvfFuncManager.h>
#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <kmonitor/client/MetricsReporter.h>
#include <suez/turing/common/SuezCavaAllocator.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
struct ColumnDataTypeTraits
{
    typedef T value_type;
};
#define REGISTER_COLUMN_DATATYPE_TRAITS(CppType, ColumnDataType) template<> \
    struct ColumnDataTypeTraits<CppType>                                \
    {                                                                   \
        typedef ColumnDataType value_type;                              \
    }
REGISTER_COLUMN_DATATYPE_TRAITS(std::string, autil::MultiChar);

class OpTestBase : public TESTBASE {
public:
    OpTestBase(): _tableName("invertedTable") {};
    virtual ~OpTestBase() {
    }
    void SetUp() override {
        TESTBASE::SetUp();
        _poolPtr.reset(new autil::mem_pool::Pool());
        _testPath = GET_TEMP_DATA_PATH() + "sql_op_index_path/";
        _bizInfo.reset(new suez::turing::BizInfo());
        if (_needBuildIndex) {
            ASSERT_NO_FATAL_FAILURE(prepareIndex());
        }
        ASSERT_NO_FATAL_FAILURE(prepareResource(_needExprResource));
   }

    void TearDown() override {
        _attributeMap.clear();
        _indexPartitionMap.clear();
        clearResource();
        _inputs.clear();
        _outputs.clear();
        TESTBASE::TearDown();
    }
    int64_t getRunId() {
        return 0;
    }
    virtual void prepareIndex() {
        prepareUserIndex();
        IE_NAMESPACE(partition)::JoinRelationMap joinMap;
        _indexApp.reset(new IE_NAMESPACE(partition)::IndexApplication);
        _indexApp->Init(_indexPartitionMap, joinMap);
    }

    virtual void prepareUserIndex() {
        if (_needKkvTable) {
            prepareKkvTable();
        } else if (_needKvTable) {
            prepareKvTable();
        } else {
            prepareInvertedTable();
        }
    }

    void prepareKkvTable() {
        std::string tableName, fields, attributes, docs, pkeyField, skeyField;
        int64_t ttl;
        prepareKkvTableData(tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartitionForKkvTable(testPath, tableName, fields, pkeyField,
                                                            skeyField, attributes, docs, ttl);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _bizInfo->_itemTableName = schemaName;
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    virtual void prepareKkvTableData(std::string &tableName, std::string &fields,
                                     std::string &pkeyField, std::string &skeyField,
                                     std::string &attributes, std::string &docs, int64_t &ttl) {
        tableName = _tableName;
        fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        pkeyField = "trigger_id";
        skeyField = "sk";
        attributes = "sk;attr2;name;value";
        docs =
            "cmd=add,sk=0,attr2=0,trigger_id=0,name=str1,value=111 aaa,ts=10000000;"
            "cmd=add,sk=1,attr2=0,trigger_id=0,name=str2,value=222 bbb,ts=20000000;"
            "cmd=add,sk=0,attr2=2,trigger_id=1,name=str3,value=333 ccc,ts=30000000;"
            "cmd=add,sk=1,attr2=2,trigger_id=1,name=str4,value=444 ddd,ts=40000000;"
            "cmd=add,sk=2,attr2=2,trigger_id=1,name=str5,value=555 eee,ts=50000000;"
            "cmd=add,sk=0,attr2=2,trigger_id=2,name=str6,value=666 fff,ts=60000000";
        ttl = std::numeric_limits<int64_t>::max();
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr
        makeIndexPartitionForKkvTable(const std::string &rootPath, const std::string &tableName,
                                      const std::string &fieldInfos, const std::string &pkeyName,
                                      const std::string &skeyName, const std::string &valueInfos,
                                      const std::string &docStrs, int64_t ttl) {
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateKKVSchema(
                tableName, fieldInfos, pkeyName, skeyName, valueInfos);
        assert(schema.get() != nullptr);
        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, rootPath, docStrs, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

    void prepareKvTable() {
        std::string tableName, fields, attributes, docs, pkeyField;
        bool enableTTL;
        int64_t ttl;
        prepareKvTableData(tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartitionForKvTable(
                testPath, tableName, fields, pkeyField,
                attributes, docs, enableTTL, ttl);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _bizInfo->_itemTableName = schemaName;
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    virtual void prepareKvTableData(std::string &tableName, std::string &fields,
                                    std::string &pkeyField, std::string &attributes,
                                    std::string &docs, bool &enableTTL, int64_t &ttl) {
        tableName = _tableName;
        fields = "attr1:uint32;attr2:string:true;pk:uint32";
        attributes = "attr1;attr2";
        docs =
            "cmd=add,attr1=0,attr2=00 aa,pk=0,ts=5000000;"
            "cmd=add,attr1=1,attr2=11 bb,pk=1,ts=10000000;"
            "cmd=add,attr1=2,attr2=22 cc,pk=2,ts=15000000;"
            "cmd=add,attr1=3,attr2=33 dd,pk=3,ts=20000000";
        pkeyField = "pk";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr
        makeIndexPartitionForKvTable(const std::string &rootPath, const std::string &tableName,
                                     const std::string &fieldInfos, const std::string &pkeyName,
                                     const std::string &valueInfos, const std::string &docStrs,
                                     bool &enableTTL, int64_t ttl) {
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateKVSchema(
                tableName, fieldInfos, pkeyName, valueInfos, enableTTL);
        assert(schema.get() != nullptr);
        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, rootPath, docStrs, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

    virtual void prepareInvertedTable() {
        std::string tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs;
        int64_t ttl;
        prepareInvertedTableData(tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _bizInfo->_itemTableName = schemaName;
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    virtual void prepareInvertedTableData(std::string &tableName,
            std::string &fields, std::string &indexes, std::string &attributes,
            std::string &summarys, std::string &truncateProfileStr,
            std::string &docs, int64_t &ttl)
    {
        tableName = _tableName;
        fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text";
        indexes = "id:primarykey64:id;index_2:text:index2;name:string:name";
        attributes = "attr1;attr2;id";
        summarys = "name";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a;"
               "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c;"
               "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop;"
               "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d";
        ttl = std::numeric_limits<int64_t>::max();
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr makeIndexPartitionForEmbeddingTable(
        const std::string &indexPath, const std::string &tableName, const std::string &schemaPath,
        const std::string &schemaFileName, const std::string &dataPath, const std::string &pluginPath,
        int64_t ttl) {
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
                IE_NAMESPACE(index_base)::SchemaAdapter::LoadSchema(schemaPath, schemaFileName);
        if (schema.get() == nullptr) {
            std::cout << "schema is nullptr" << std::endl;
        }
        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        options.TEST_mQuickExit = true;
        std::string dataStr;
        IE_NAMESPACE(storage)::FileSystemWrapper::AtomicLoad(dataPath, dataStr);

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
                IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                    schema, indexPath, dataStr, options, pluginPath, true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

    virtual void setPartitionReader(const ::tensorflow::QueryResourcePtr &queryResource) {
        if (_indexApp) {
            queryResource->setIndexSnapshot(_indexApp->CreateSnapshot());
        }
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr
        makeIndexPartitionForIndexTable(const std::string &rootPath, const std::string &tableName,
                                        const std::string &fieldInfos, const std::string& indexInfos,
                                        const std::string& attrInfos, const std::string& summaryInfos,
                                        const std::string& docStrs) {
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
            tableName, fieldInfos, indexInfos, attrInfos, summaryInfos);
        assert(schema.get() != nullptr);
        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
                IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(schema, rootPath, docStrs);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

    void prepareResource(bool needExprResource = false) {
        prepareSessionResource(needExprResource);
        auto queryResource = prepareQueryResource(needExprResource);
        _sessionResource->addQueryResource(0, queryResource);  // runId = 0;
        _sessionResource->bizInfo = *_bizInfo;
        ASSERT_NO_FATAL_FAILURE(prepareSqlResource());
    }

    void clearResource() {
        clearQueryResource();
        clearSqlResource();
        _sessionResource.reset();
    }

    IE_NAMESPACE(partition)::IndexPartitionPtr makeIndexPartitionForInvertedTable(
            const std::string &rootPath, const std::string &tableName,
            const std::string &fields, const std::string &indexes,
            const std::string &attributes, const std::string &summarys,
            const std::string &truncateProfileStr, const std::string &docs,
            int64_t ttl)
    {
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                    tableName, fields, indexes, attributes, summarys, truncateProfileStr);
        assert(schema.get() != nullptr);
        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        options.TEST_mQuickExit = true;
        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                    schema, rootPath, docs, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

    ::tensorflow::QueryResourcePtr prepareQueryResource(bool needExprResource) {
        ::tensorflow::QueryResourcePtr queryResource = createQueryResource();
        setPartitionReader(queryResource);
        if (_needTracer) {
            queryResource->setTracer(suez::turing::TracerPtr(new suez::turing::Tracer()));
        }
        if (_needTerminator) {
            queryResource->setTimeoutTerminator(suez::turing::TimeoutTerminatorPtr(
                    new suez::turing::TimeoutTerminator(-1, queryResource->startTime)));  // no timeout
        }
        queryResource->setPool(&_pool);
        queryResource->_cavaAllocator.reset(
                new suez::turing::SuezCavaAllocator(&_pool, 1024 * 1024));
        if (needExprResource) {
            prepareExprQueryResource(queryResource);
        }
        _metricsReporter.reset(new kmonitor::MetricsReporter("", "", {}));
        queryResource->setQueryMetricsReporter(_metricsReporter);
        return queryResource;
    }

    void clearQueryResource() {
        if (_sessionResource) {
            _sessionResource->removeQueryResource(0);
            _sessionResource->queryResourceVec.clear();
        }
    }

    virtual ::tensorflow::QueryResourcePtr createQueryResource() {
        ::tensorflow::QueryResourcePtr queryResource(new ::tensorflow::QueryResource());
        return queryResource;
    }

    void prepareExprQueryResource(::tensorflow::QueryResourcePtr queryResource) {
        queryResource->setProviderCreator(suez::turing::ProviderCreatorPtr(new suez::turing::ProviderCreator()));
    }

    void prepareSessionResource(bool needExprResource = false) {
        _sessionResource = createSessionResource(10);
        suez::ServiceInfo serviceInfo;
        _sessionResource->serviceInfo = serviceInfo;
        _sessionResource->indexApplication = _indexApp;
        _sessionResource->resourceReader;
        if (_needBuildIndex) {
            _sessionResource->dependencyTableInfoMap[_tableName] =
                suez::turing::TableInfoConfigurator::createFromIndexApp(_bizInfo->_itemTableName, _indexApp);
        }
        if (needExprResource) {
            prepareExprSessionResource();
        }
    }

    virtual ::tensorflow::SessionResourcePtr createSessionResource(
            uint32_t maxOutstandingCount) {
        ::tensorflow::SessionResourcePtr sessionResource(
                new tensorflow::SessionResource(maxOutstandingCount));
        return sessionResource;
    }

    void prepareExprSessionResource() {
        _sessionResource->sorterManager.reset(new suez::turing::SorterManager(""));
        _sessionResource->functionInterfaceCreator.reset(new suez::turing::FunctionInterfaceCreator());
        _sessionResource->functionInterfaceCreator->init(suez::turing::FuncConfig(), {});

        _sessionResource->exprConfigManager.reset(new suez::turing::ExprConfigManager());
        _sessionResource->bizInfo = *_bizInfo;
        _sessionResource->tableInfo = suez::turing::TableInfoConfigurator::createFromIndexApp(_bizInfo->_itemTableName, _indexApp);
    }

    tensorflow::QueryResourcePtr getQueryResource() {
        return _sessionResource->getQueryResource(0);
    }
    void prepareSqlResource() {
        turing::SqlSessionResource *sqlResource = new turing::SqlSessionResource();
        sqlResource->aggFuncManager.reset(new AggFuncManager());
        ASSERT_TRUE(sqlResource->aggFuncManager->init("", HA3_NS(config)::SqlAggPluginConfig()));
        ASSERT_TRUE(sqlResource->aggFuncManager->registerFunctionInfos());

        sqlResource->tvfFuncManager.reset(new TvfFuncManager());
        ASSERT_TRUE(sqlResource->tvfFuncManager->init("", HA3_NS(config)::SqlTvfPluginConfig()));

        _sessionResource->sharedObjectMap.setWithDelete(turing::SqlSessionResource::name(),
                sqlResource, suez::turing::DeleteLevel::Delete);
        _sqlResource = sqlResource;
        _searchService.reset(new multi_call::SearchService());
        _sqlBizResource.reset(new SqlBizResource(_sessionResource));
        _sqlQueryResource.reset(new SqlQueryResource(getQueryResource()));
        multi_call::QuerySessionPtr querySession(new multi_call::QuerySession(_searchService));
        _sqlQueryResource->setGigQuerySession(querySession);
        _resourceMap[_sqlBizResource->getResourceName()] = _sqlBizResource.get();
        _resourceMap[_sqlQueryResource->getResourceName()] = _sqlQueryResource.get();
        _resourceMap[navi::RESOURCE_MEMORY_POOL_URI] = &_memPoolResource;
    }

    void clearSqlResource() {
        _resourceMap.clear();
        _sqlQueryResource.reset();
        _sqlBizResource.reset();
    }

    TablePtr getTable(navi::DataPtr data) {
        TablePtr table = std::dynamic_pointer_cast<Table>(data);
        return table;
    }

    template <typename T>
    ColumnData<T> getColumnData(const TablePtr &table, const std::string &colName) {
        ColumnPtr column = table->getColumn(colName);
        ASSERT_TRUE(column != NULL);
        ColumnData<T> columnData = column->getColumnData<T>();
        return columnData;
    }

    std::vector<matchdoc::MatchDoc> getMatchDocs(matchdoc::MatchDocAllocatorPtr &allocator, size_t size)
    {
        allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr));
        return allocator->batchAllocate(size);
    }

    std::vector<matchdoc::MatchDoc> getMatchDocsUseSub(matchdoc::MatchDocAllocatorPtr &allocator, size_t size)
    {
        allocator.reset(new matchdoc::MatchDocAllocator(_poolPtr, true));
        return allocator->batchAllocate(size);
    }

    template <typename T>
    void extendMatchDocAllocator(matchdoc::MatchDocAllocatorPtr allocator,
                                 std::vector<matchdoc::MatchDoc> docs,
                                 const std::string &fieldName,
                                 const std::vector<T> &values)
    {
        auto pRef = allocator->declare<T>(fieldName, SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto value = values[i];
            pRef->set(docs[i], value);
        }
    }

    void extendMatchDocAllocator(matchdoc::MatchDocAllocatorPtr allocator,
                                 std::vector<matchdoc::MatchDoc> docs,
                                 const std::string &fieldName,
                                 const std::vector<std::string> &values)
    {
        auto pRef = allocator->declare<autil::MultiChar>(fieldName, SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto value = values[i];
            auto p = autil::MultiValueCreator::createMultiValueBuffer(
                    value.data(), value.size(), &_pool);
            pRef->getReference(docs[i]).init(p);
        }
    }

    template <typename T>
    void extendMultiValueMatchDocAllocator(
            matchdoc::MatchDocAllocatorPtr allocator,
            std::vector<matchdoc::MatchDoc> docs,
            const std::string &fieldName,
            const std::vector<std::vector<T>> &values)
    {
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        auto pRef = allocator->declare<autil::MultiValueType<ColumnDataType> >(fieldName, SL_ATTRIBUTE);
        allocator->extend();
        ASSERT_EQ(values.size(), docs.size());
        for (size_t i = 0; i < values.size(); ++i) {
            auto p = autil::MultiValueCreator::createMultiValueBuffer(values[i], &_pool);
            pRef->getReference(docs[i]).init(p);
        }
    }

    template<typename T>
    void checkValueEQ(const T &expected, const T &actual,
                      const std::string &msg = "")
    {
        ASSERT_EQ(expected, actual) << msg;
    }
    void checkValueEQ(const std::string &expected,
                      const autil::MultiChar &actual,
                      const std::string &msg = "")
    {
        autil::MultiChar p(autil::MultiValueCreator::createMultiValueBuffer(
                            expected.data(), expected.size(), &_pool));
        ASSERT_EQ(p, actual) << msg;
    }

    void checkValueEQ(const double &expected,
                      const double &actual,
                      const std::string &msg = "")
    {
        ASSERT_DOUBLE_EQ(expected, actual) << msg;
    }

    template<typename T>
    void checkOutputColumn(const TablePtr &table,
                           const std::string &columnName,
                           const std::vector<T> &expects)
    {
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        ColumnPtr column = table->getColumn(columnName);
        ASSERT_TRUE(column != NULL) << "columnName: " << columnName;
        auto result = column->getColumnData<ColumnDataType>();
        ASSERT_TRUE(result != NULL) <<
            "columnType not match, columnName: " << columnName;

        std::ostringstream oss;
        oss << "expected: " << autil::StringUtil::toString(expects, ",") << std::endl;
        oss << "actual: ";
        for (size_t i = 0; i < table->getRowCount(); ++i) {
            oss << result->toString(i) << ",";
        }
        oss << std::endl;
        std::string msg = oss.str();

        ASSERT_EQ(expects.size(), table->getRowCount()) << msg;
        for (size_t i = 0; i < expects.size(); i++) {
            ASSERT_NO_FATAL_FAILURE(checkValueEQ(expects[i], result->get(i), msg));
        }
    }

    void checkOutputColumn(const TablePtr &table,
                           const std::string &columnName,
                           const std::vector<std::string> &expects)
    {
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<std::string>(table, columnName, expects));
    }

    template<typename T>
    void checkOutputMultiColumn(const TablePtr &table,
                                const std::string &columnName,
                                const std::vector<std::vector<T>> &expects)
    {
        typedef typename ColumnDataTypeTraits<T>::value_type ColumnDataType;
        ASSERT_EQ(expects.size(), table->getRowCount());
        ColumnPtr column = table->getColumn(columnName);
        ASSERT_TRUE(column != NULL);
        auto result = column->getColumnData<autil::MultiValueType<ColumnDataType>>();
        ASSERT_TRUE(result != NULL);
        for (size_t i = 0; i < expects.size(); i++) {
            autil::MultiValueType<ColumnDataType> p(autil::MultiValueCreator::createMultiValueBuffer(expects[i], &_pool));
            ASSERT_EQ(p, result->get(i)) << p << "|" << result->get(i);
        }
    }

    void checkOutputMultiColumn(const TablePtr &table,
                                const std::string &columnName,
                                const std::vector<std::vector<std::string>> &expects)
    {
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<std::string>(table, columnName, expects));
    }

    template<typename T>
    void checkMultiValue(const std::vector<T> &expects, const autil::MultiValueType<T> &actual) {
        autil::MultiValueType<T> p(autil::MultiValueCreator::createMultiValueBuffer(expects, &_pool));
        ASSERT_EQ(p, actual);
    }

    navi::DataPtr createTable(matchdoc::MatchDocAllocatorPtr allocator,
                         std::vector<matchdoc::MatchDoc> docs)
    {
        TablePtr ret(new Table(docs, allocator));
        return std::dynamic_pointer_cast<navi::Data>(ret);
    }

    static void checkTraceCount(size_t expectedNums, const std::string &filter,
                                const std::vector<std::string> &traces)
    {
        size_t count = std::count_if(
                traces.begin(), traces.end(),
                [&](auto &text) { return text.find(filter) != std::string::npos; });
        ASSERT_EQ(expectedNums, count) << "filter: " << filter;
    }

    template<typename T>
    void checkVector(const std::vector<T> &expects, const std::vector<T> &actuals)
    {
        ASSERT_EQ(expects.size(), actuals.size());
        for (size_t i = 0; i < expects.size(); i++) {
            ASSERT_EQ(expects[i], actuals[i]);
        }
    }

public:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    std::string _testPath;
    std::string _tableName;
    ::tensorflow::SessionResourcePtr _sessionResource;
    multi_call::SearchServicePtr _searchService;
    SqlBizResourcePtr _sqlBizResource;
    SqlQueryResourcePtr _sqlQueryResource;
    turing::SqlSessionResource *_sqlResource;
    navi::MemoryPoolResource _memPoolResource;
    std::map<std::string, navi::Resource*> _resourceMap;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    suez::turing::BizInfoPtr _bizInfo;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    bool _needBuildIndex = false;
    bool _needKvTable = false;
    bool _needKkvTable = false;
    bool _needExprResource = false;
    bool _needTracer = true;
    bool _needTerminator = true;
    std::vector<std::pair<navi::DataPtr, bool>> _inputs;
    std::vector<std::pair<navi::DataPtr, bool>> _outputs;
    autil::legacy::json::JsonMap _attributeMap;
};


#define CHECK_TRACE_COUNT(expectNums, filter, traces)                   \
    ASSERT_NO_FATAL_FAILURE(                                            \
            OpTestBase::checkTraceCount(expectNums, filter, traces))

#define IF_FAILURE_RET_NULL(expr)                 \
    do {                                          \
        auto wrapper = [&]() {                    \
            ASSERT_NO_FATAL_FAILURE(expr);        \
        };                                        \
        wrapper();                                \
        if (HasFatalFailure()) {                  \
            return nullptr;                       \
        }                                         \
    } while (false)

END_HA3_NAMESPACE(sql);
