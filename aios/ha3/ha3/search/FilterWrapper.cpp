#include <ha3/search/FilterWrapper.h>
#include <matchdoc/SubDocAccessor.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FilterWrapper);

FilterWrapper::FilterWrapper() 
    : _joinFilter(NULL)
    , _subDocFilter(NULL)
    , _filter(NULL)
    , _subExprFilter(NULL)
{ 
}

FilterWrapper::~FilterWrapper() { 
    POOL_DELETE_CLASS(_joinFilter);
    POOL_DELETE_CLASS(_subDocFilter);
    POOL_DELETE_CLASS(_filter);
    POOL_DELETE_CLASS(_subExprFilter);
}

bool SubDocFilter::pass(matchdoc::MatchDoc matchDoc) {
    return !_subDocAccessor->allSubDocSetDeletedFlag(matchDoc);
}

END_HA3_NAMESPACE(search);

