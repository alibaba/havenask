#ifndef ISEARCH_EXTRARANKPROCESSOR_H
#define ISEARCH_EXTRARANKPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/search/MatchDocSearcherProcessorResource.h>
#include <ha3/sorter/SorterResource.h>
#include <ha3/sorter/SorterProvider.h>
#include <suez/turing/expression/plugin/SorterWrapper.h>
#include <suez/turing/expression/plugin/SorterManager.h>

BEGIN_HA3_NAMESPACE(request);
class Request;
END_HA3_NAMESPACE(request);

BEGIN_HA3_NAMESPACE(search);

class ExtraRankProcessor
{
public:
    ExtraRankProcessor(SearchCommonResource &resource,
                       SearchPartitionResource &partitionResouce,
                       SearchProcessorResource &processorResource);
    ~ExtraRankProcessor();

private:
    ExtraRankProcessor(const ExtraRankProcessor &);
    ExtraRankProcessor& operator=(const ExtraRankProcessor &);

public:
    bool init(const common::Request *request);
    void process(const common::Request *request,
                 InnerSearchResult& result,
                 uint32_t extraRankCount) ;
private:
    sorter::SorterResource createSorterResource(const common::Request *request);
    suez::turing::SorterWrapper *createFinalSorterWrapper(
            const common::Request *request,
            const suez::turing::SorterManager *sorterManager) const;
private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    SearchProcessorResource &_processorResource;
    sorter::SorterProvider *_sorterProvider;
    suez::turing::SorterWrapper *_finalSorterWrapper;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_EXTRARANKPROCESSOR_H
