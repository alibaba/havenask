#ifndef ISEARCH_HA3PROVIDERCREATOR_H
#define ISEARCH_HA3PROVIDERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/provider/ProviderCreator.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/sorter/SorterProvider.h>
#include <ha3/config/IndexInfoHelper.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <ha3/search/SearchCommonResource.h>

BEGIN_HA3_NAMESPACE(search);

class Ha3ProviderCreator : public suez::turing::ProviderCreator
{
public:
    Ha3ProviderCreator(const common::Request *request,
                       const sorter::SorterLocation &location,
                       SearchCommonResource &resource,
                       SearchPartitionResource &partitionResource,
                       SearchRuntimeResource &runtimeResource);
    ~Ha3ProviderCreator() {}
private:
    Ha3ProviderCreator(const Ha3ProviderCreator &);
    Ha3ProviderCreator& operator=(const Ha3ProviderCreator &);
public:
    suez::turing::ScoringProviderPtr createScoringProvider() override {
        suez::turing::ScoringProviderPtr provider(new rank::ScoringProvider(_rankResource));
        return provider;
    }
    suez::turing::FunctionProviderPtr createFunctionProvider() override {
        suez::turing::FunctionProviderPtr provider(new func_expression::FunctionProvider(_functionResource));
        return provider;
    }
    suez::turing::SorterProviderPtr createSorterProvider() override {
        suez::turing::SorterProviderPtr provider(new sorter::SorterProvider(_sorterResource));
        return provider;
    }
public:
    static RankResource createRankResource(const common::Request *request,
            const config::IndexInfoHelper *indexInfoHelper,
            const rank::RankProfile *rankProfile,
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource);
    static sorter::SorterResource createSorterResource(const common::Request *request,
            const sorter::SorterLocation &location,
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource,
            SearchRuntimeResource &runtimeResource);
    static func_expression::FunctionResource createFunctionResource(const common::Request *request,
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource);
private:
    func_expression::FunctionResource _functionResource;
    search::RankResource _rankResource;
    sorter::SorterResource _sorterResource;
    config::IndexInfoHelperPtr _indexInfoHelper;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Ha3ProviderCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_HA3PROVIDERCREATOR_H
