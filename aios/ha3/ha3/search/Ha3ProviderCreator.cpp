#include <ha3/search/Ha3ProviderCreator.h>
#include <ha3/config/LegacyIndexInfoHelper.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(sorter);
USE_HA3_NAMESPACE(func_expression);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, Ha3ProviderCreator);

Ha3ProviderCreator::Ha3ProviderCreator(const common::Request *request,
                   const sorter::SorterLocation &location,
                   SearchCommonResource &resource,
                   SearchPartitionResource &partitionResource,
                   SearchRuntimeResource &runtimeResource)
    : _indexInfoHelper(new config::LegacyIndexInfoHelper(resource.tableInfo ?
                    resource.tableInfo->getIndexInfos() : NULL))
{
    _rankResource = createRankResource(request, _indexInfoHelper.get(),
            runtimeResource.rankProfile, resource, partitionResource);
    _sorterResource = createSorterResource(request, location,
            resource, partitionResource, runtimeResource);
    _functionResource = createFunctionResource(request, resource, partitionResource);
}

RankResource Ha3ProviderCreator::createRankResource(const common::Request *request,
        const config::IndexInfoHelper *indexInfoHelper,
        const rank::RankProfile *rankProfile,
        SearchCommonResource &resource,
        SearchPartitionResource &partitionResource)
{
    RankResource rankResource;
    rankResource.pool = resource.pool;
    rankResource.request = request;
    rankResource.matchDataManager = &partitionResource.matchDataManager;
    rankResource.globalMatchData.setDocCount(partitionResource.getTotalDocCount());
    rankResource.indexInfoHelper = indexInfoHelper;
    rankResource.requestTracer = resource.tracer;
    rankResource.boostTable = rankProfile ?
                              rankProfile->getFieldBoostTable() : NULL;
    if (partitionResource.indexPartitionReaderWrapper) {
        rankResource.indexReaderPtr =
            partitionResource.indexPartitionReaderWrapper->getReader()->GetIndexReader();
    }
    rankResource.attrExprCreator = partitionResource.attributeExpressionCreator.get();
    rankResource.dataProvider = &resource.dataProvider;
    rankResource.matchDocAllocator = resource.matchDocAllocator;
    rankResource.fieldInfos = resource.tableInfo ?
                              resource.tableInfo->getFieldInfos() : NULL;
    rankResource.queryTerminator = resource.timeoutTerminator;
    rankResource.kvpairs = request && request->getConfigClause() ?
                           &request->getConfigClause()->getKVPairs() : NULL;
    rankResource.partitionReaderSnapshot = partitionResource.partitionReaderSnapshot.get();
    return rankResource;
}

SorterResource Ha3ProviderCreator::createSorterResource(const common::Request *request,
        const sorter::SorterLocation &location,
        SearchCommonResource &resource,
        SearchPartitionResource &partitionResource,
        SearchRuntimeResource &runtimeResource)
{
    sorter::SorterResource sorterResource;
    sorterResource.location = location;
    auto scoreRef = resource.matchDocAllocator->findReference<score_t>(SCORE_REF);
    sorterResource.scoreExpression = NULL;
    if (scoreRef) {
        sorterResource.scoreExpression =
            AttributeExpressionFactory::createAttributeExpression(scoreRef, resource.pool);
    }
    sorterResource.requestTracer = resource.tracer;
    sorterResource.pool = resource.pool;
    sorterResource.request = request;
    sorterResource.globalMatchData.setDocCount(partitionResource.getTotalDocCount());
    sorterResource.matchDataManager = &partitionResource.matchDataManager;
    sorterResource.attrExprCreator = partitionResource.attributeExpressionCreator.get();
    sorterResource.dataProvider = &resource.dataProvider;
    sorterResource.matchDocAllocator = resource.matchDocAllocator;
    sorterResource.fieldInfos = resource.tableInfo ?
                                resource.tableInfo->getFieldInfos() : NULL;
    // support sorter update requiredTopk!!.
    sorterResource.requiredTopk = &runtimeResource.docCountLimits.requiredTopK;
    sorterResource.comparatorCreator = runtimeResource.comparatorCreator.get();
    sorterResource.sortExprCreator = runtimeResource.sortExpressionCreator.get();
    sorterResource.queryTerminator = resource.timeoutTerminator;
    sorterResource.kvpairs = request && request->getConfigClause() ?
                             &request->getConfigClause()->getKVPairs() : NULL;
    sorterResource.partitionReaderSnapshot = partitionResource.partitionReaderSnapshot.get();
    return sorterResource;
}

FunctionResource Ha3ProviderCreator::createFunctionResource(const common::Request *request,
        SearchCommonResource &resource,
        SearchPartitionResource &partitionResource)
{
    func_expression::FunctionResource funcResource;
    funcResource.pool = resource.pool;
    funcResource.request = request;
    funcResource.globalMatchData.setDocCount(0);
    if (partitionResource.indexPartitionReaderWrapper) {
        auto partitionInfo = partitionResource.indexPartitionReaderWrapper->getPartitionInfo();
        if (partitionInfo) {
            funcResource.globalMatchData.setDocCount(partitionInfo->GetTotalDocCount());
        }
    }
    funcResource.matchDataManager = &partitionResource.matchDataManager;
    funcResource.attrExprCreator = partitionResource.attributeExpressionCreator.get();
    funcResource.dataProvider = &resource.dataProvider;
    funcResource.matchDocAllocator = resource.matchDocAllocator;
    funcResource.fieldInfos = resource.tableInfo ?
                              resource.tableInfo->getFieldInfos() : NULL;
    funcResource.queryTerminator = resource.timeoutTerminator;
    funcResource.requestTracer = resource.tracer;
    funcResource.kvpairs = request && request->getConfigClause() ?
                           &request->getConfigClause()->getKVPairs() : NULL;
    funcResource.partitionReaderSnapshot = partitionResource.partitionReaderSnapshot.get();
    return funcResource;
}

END_HA3_NAMESPACE(search);
