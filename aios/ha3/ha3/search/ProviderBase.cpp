#include <ha3/search/ProviderBase.h>
#include <ha3/common/Tracer.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, ProviderBase);

ProviderBase::ProviderBase() 
    : _searchPluginResource(NULL)
    , _scope(AE_DEFAULT)
{ 
}

ProviderBase::~ProviderBase() { 
}

void ProviderBase::setSearchPluginResource(SearchPluginResource *resource) {
    _searchPluginResource = resource;
}

int32_t ProviderBase::convertRankTraceLevel(const common::Request *request) {
    if (!request) {
        return ISEARCH_TRACE_NONE;
    }
    const auto *configClause = request->getConfigClause();
    if (!configClause) {
        return ISEARCH_TRACE_NONE;
    }
    return common::Tracer::convertLevel(configClause->getRankTrace());
}

const GlobalMatchData *ProviderBase::getGlobalMatchData() const {
    return &_searchPluginResource->globalMatchData;
}

const common::Request *ProviderBase::getRequest() const {
    return _searchPluginResource->request;
}

common::AggResultReader ProviderBase::getAggResultReader() const {
    return common::AggResultReader(_aggResultsPtr);
}

void ProviderBase::setErrorMessage(const std::string &errorMsg) {
    _errorMsg = errorMsg;
}

void ProviderBase::getQueryTermMetaInfo(rank::MetaInfo *metaInfo) const {
    _searchPluginResource->matchDataManager->getQueryTermMetaInfo(metaInfo);
}

common::QueryTerminator *ProviderBase::getQueryTerminator() {
    return _searchPluginResource->queryTerminator;
}

#define REQUIRE_MATCHDATA_IMPL(Type, Name, RefName)                     \
const matchdoc::Reference<Type>* ProviderBase::require##Name() {        \
    auto subDocDisplayType = SUB_DOC_DISPLAY_NO;                        \
    if (_searchPluginResource->request) {                               \
        auto request = _searchPluginResource->request;                  \
        auto clause = request->getConfigClause();                       \
        if (clause) {                                                   \
            subDocDisplayType = clause->getSubDocDisplayType();         \
        }                                                               \
    }                                                                   \
    auto allocator = _searchPluginResource->matchDocAllocator.get();    \
    auto ret = allocator->findReference<Type>(RefName);                 \
    if (ret) {                                                          \
        return ret;                                                     \
    }                                                                   \
    auto manager = _searchPluginResource->matchDataManager;             \
    if (manager) {                                                      \
        manager->setAttributeExprScope(getAttributeExprScope());        \
        return manager->require##Name(allocator, RefName,               \
                subDocDisplayType, _searchPluginResource->pool);        \
    }                                                                   \
    return nullptr;                                                     \
}

REQUIRE_MATCHDATA_IMPL(MatchData, MatchData, common::MATCH_DATA_REF);
REQUIRE_MATCHDATA_IMPL(SimpleMatchData, SimpleMatchData, common::SIMPLE_MATCH_DATA_REF);
REQUIRE_MATCHDATA_IMPL(SimpleMatchData, SubSimpleMatchData, common::SUB_SIMPLE_MATCH_DATA_REF);
REQUIRE_MATCHDATA_IMPL(Ha3MatchValues, MatchValues, common::MATCH_VALUE_REF);

SearchPluginResource *ProviderBase::getSearchPluginResource() const {
    return _searchPluginResource;
}

END_HA3_NAMESPACE(search);

