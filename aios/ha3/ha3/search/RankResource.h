#ifndef ISEARCH_RANKRESOURCE_H
#define ISEARCH_RANKRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <assert.h>
#include <ha3/common/Request.h>
#include <ha3/common/DataProvider.h>
#include <ha3/search/QueryExecutor.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/rank/GlobalMatchData.h>
#include <suez/turing/expression/util/FieldBoostTable.h>
#include <ha3/config/IndexInfoHelper.h>
#include <suez/turing/expression/util/FieldInfos.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <indexlib/index/normal/summary/summary_reader.h>
#include <ha3/search/SearchPluginResource.h>
BEGIN_HA3_NAMESPACE(search);

class RankResource : public SearchPluginResource
{
public:
    RankResource() {
        boostTable = NULL;
        indexInfoHelper = NULL;
        cavaAllocator = NULL;
    }
public:
    const suez::turing::FieldBoostTable *boostTable;
    const config::IndexInfoHelper *indexInfoHelper;
    index::IndexReaderPtr indexReaderPtr;//for get section reader
    suez::turing::SuezCavaAllocator *cavaAllocator;
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_RANKRESOURCE_H
