#ifndef ISEARCH_SEARCHPARTITIONRESOURCE_H
#define ISEARCH_SEARCHPARTITIONRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/common/Result.h>
#include <ha3/common/Request.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/search/DocCountLimits.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <ha3/rank/ComparatorCreator.h>
#include <cava/codegen/CavaModule.h>
#include <ha3/common/GlobalVariableManager.h>

BEGIN_HA3_NAMESPACE(search);

class SearchCommonResource
{
public:
    SearchCommonResource(
            autil::mem_pool::Pool *pool_,
            const suez::turing::TableInfoPtr &tableInfoPtr_,
            monitor::SessionMetricsCollector *sessionMetricsCollector_,
            common::TimeoutTerminator *timeoutTerminator_,
            common::Tracer* tracer_,
            const suez::turing::FunctionInterfaceCreator *funcCreator_,
            const suez::turing::CavaPluginManagerPtr cavaPluginManagerPtr_,
            const common::Request *request,
            suez::turing::SuezCavaAllocator *cavaAllocator_,
            const std::map<size_t, ::cava::CavaJitModulePtr> &cavaJitModules_,
            common::Ha3MatchDocAllocatorPtr matchDocAllocator_ =
            common::Ha3MatchDocAllocatorPtr())
        : pool(pool_)
        , tableInfoPtr(tableInfoPtr_)
        , tableInfo(tableInfoPtr.get())
        , sessionMetricsCollector(sessionMetricsCollector_)
        , timeoutTerminator(timeoutTerminator_)
        , tracer(tracer_)
        , functionCreator(funcCreator_)
        , cavaPluginManagerPtr(cavaPluginManagerPtr_)
        , errorResult(new common::MultiErrorResult())
        , dataProvider()
        , cavaAllocator(cavaAllocator_)
        , cavaJitModules(cavaJitModules_)
    {
        if (matchDocAllocator_) {
            matchDocAllocator = matchDocAllocator_;
        } else {
            matchDocAllocator.reset(new common::Ha3MatchDocAllocator(
                            pool, useSubDoc(request, tableInfo)));
        }
    }

    ~SearchCommonResource() {
    }
private:
    static bool useSubDoc(const common::Request *request, const suez::turing::TableInfo *tableInfo) {
        if (!request) {
            return false;
        }
        common::ConfigClause *configClause = request->getConfigClause();
        if (configClause != NULL
            && configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO
            && tableInfo && tableInfo->hasSubSchema())
        {
            return true;
        }
        return false;
    }
public:
    autil::mem_pool::Pool *pool = nullptr;
    suez::turing::TableInfoPtr tableInfoPtr;
    const suez::turing::TableInfo *tableInfo = nullptr;
    monitor::SessionMetricsCollector *sessionMetricsCollector = nullptr;
    common::TimeoutTerminator *timeoutTerminator = nullptr;
    common::Tracer* tracer = nullptr;
    const suez::turing::FunctionInterfaceCreator *functionCreator = nullptr;
    const suez::turing::CavaPluginManagerPtr cavaPluginManagerPtr;
    common::MultiErrorResultPtr errorResult;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator;
    common::DataProvider dataProvider;
    suez::turing::SuezCavaAllocator *cavaAllocator = nullptr;
    const std::map<size_t, ::cava::CavaJitModulePtr> &cavaJitModules;
    std::vector<common::GlobalVariableManagerPtr> paraGlobalVariableManagers;
};
HA3_TYPEDEF_PTR(SearchCommonResource);

class SearchPartitionResource
{
public:
    SearchPartitionResource(
            SearchCommonResource& resource,
            const common::Request *request,
            const IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper_,
            const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &partitionReaderSnapshot_)
        : mainTable(indexPartitionReaderWrapper_ ?
                    indexPartitionReaderWrapper_->getReader()->GetSchema()->GetSchemaName() : "")
        , indexPartitionReaderWrapper(indexPartitionReaderWrapper_)
        , partitionReaderSnapshot(partitionReaderSnapshot_)
        , functionProvider(createFunctionResource(resource, request))
        , attributeExpressionCreator(new suez::turing::AttributeExpressionCreator(
                        resource.pool,
                        resource.matchDocAllocator.get(),
                        mainTable,
                        partitionReaderSnapshot.get(),
                        resource.tableInfoPtr,
                        (request && request->getVirtualAttributeClause()
                                ? request->getVirtualAttributeClause()->getVirtualAttributes()
                                : suez::turing::VirtualAttributes()),
                        resource.functionCreator,
                        resource.cavaPluginManagerPtr,
                        &functionProvider))
    {
    }
    ~SearchPartitionResource() {
        attributeExpressionCreator.reset();
    }

    int32_t getTotalDocCount() const {
        if (indexPartitionReaderWrapper) {
            return indexPartitionReaderWrapper->getPartitionInfo()->GetTotalDocCount();
        }
        return 0;
    }

private:
    SearchPartitionResource(const SearchPartitionResource &);
    SearchPartitionResource& operator=(const SearchPartitionResource &);
private:
    func_expression::FunctionResource createFunctionResource(
            SearchCommonResource& resource,
            const common::Request *request)
    {
        func_expression::FunctionResource funcResource;
        funcResource.pool = resource.pool;
        funcResource.request = request;
        funcResource.globalMatchData.setDocCount(getTotalDocCount());
        funcResource.matchDataManager = &matchDataManager;
        funcResource.attrExprCreator = attributeExpressionCreator.get();
        funcResource.dataProvider = &resource.dataProvider;
        funcResource.matchDocAllocator = resource.matchDocAllocator;
        funcResource.fieldInfos = resource.tableInfo ? resource.tableInfo->getFieldInfos() : NULL;
        funcResource.queryTerminator = resource.timeoutTerminator;
        funcResource.requestTracer = resource.tracer;
        funcResource.cavaAllocator = resource.cavaAllocator;
        funcResource.kvpairs = request && request->getConfigClause() ?
                               &request->getConfigClause()->getKVPairs() : NULL;
        funcResource.partitionReaderSnapshot = partitionReaderSnapshot.get();

        return funcResource;
    }

public:
    std::string mainTable;
    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr partitionReaderSnapshot;
    MatchDataManager matchDataManager;

    func_expression::FunctionProvider functionProvider;
    suez::turing::AttributeExpressionCreatorPtr attributeExpressionCreator;
};

HA3_TYPEDEF_PTR(SearchPartitionResource);

struct SearchRuntimeResource {
    SortExpressionCreatorPtr sortExpressionCreator;
    rank::ComparatorCreatorPtr comparatorCreator;
    DocCountLimits docCountLimits;
    const rank::RankProfile *rankProfile = nullptr;
};

HA3_TYPEDEF_PTR(SearchRuntimeResource);
END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHPARTITIONRESOURCE_H
