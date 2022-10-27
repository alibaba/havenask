#ifndef ISEARCH_RERANKPROCESSOR_H
#define ISEARCH_RERANKPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/MatchDocSearcherProcessorResource.h>
#include <ha3/search/InnerSearchResult.h>

BEGIN_HA3_NAMESPACE(rank);
class RankProfile;
class ComboComparator;
END_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class MatchDocScorers;

class RerankProcessor
{
public:
    RerankProcessor(SearchCommonResource &resource,
                    SearchPartitionResource &partitionResource,
                    SearchProcessorResource &processorResource);
    ~RerankProcessor();

private:
    RerankProcessor(const RerankProcessor &);
    RerankProcessor& operator=(const RerankProcessor &);

public:
    bool init(const common::Request* request);
    void process(const common::Request *request,
                 bool ranked, const rank::ComboComparator *rankComp,
                 InnerSearchResult& result);

private:
    bool createMatchDocScorers(const common::Request* reques);
    void sortMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect,
                       const rank::ComboComparator *rankComp);

private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    SearchProcessorResource &_processorResource;
    MatchDocScorers *_matchDocScorers;
    const config::IndexInfoHelper *_indexInfoHelper;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RerankProcessor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_RERANKPROCESSOR_H
