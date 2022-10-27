#ifndef ISEARCH_SEARCHPLUGINRESOURCE_H
#define ISEARCH_SEARCHPLUGINRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/DataProvider.h>
#include <suez/turing/expression/framework/AttributeExpressionCreatorBase.h>
#include <ha3/rank/GlobalMatchData.h>
#include <suez/turing/expression/util/FieldBoostTable.h>
#include <suez/turing/expression/util/FieldInfos.h>
#include <suez/turing/common/SuezCavaAllocator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/search/MatchDataManager.h>
#include <ha3/common/QueryTerminator.h>
#include <indexlib/partition/partition_reader_snapshot.h>
BEGIN_HA3_NAMESPACE(search);

class SearchPluginResource
{
public:
    autil::mem_pool::Pool *pool = nullptr;
    suez::turing::SuezCavaAllocator *cavaAllocator = nullptr;
    const common::Request *request = nullptr;
    rank::GlobalMatchData globalMatchData;
    suez::turing::AttributeExpressionCreatorBase *attrExprCreator = nullptr;
    common::DataProvider *dataProvider = nullptr;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator;
    const suez::turing::FieldInfos *fieldInfos = nullptr;
    MatchDataManager *matchDataManager = nullptr;
    common::QueryTerminator *queryTerminator = nullptr;
    common::Tracer *requestTracer = nullptr;
    const KeyValueMap *kvpairs = nullptr;
    IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot = nullptr;
};

HA3_TYPEDEF_PTR(SearchPluginResource);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHPLUGINRESOURCE_H
