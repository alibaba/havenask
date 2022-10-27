#include <ha3/search/MultiTermOrQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MultiTermOrQueryExecutor);

MultiTermOrQueryExecutor::MultiTermOrQueryExecutor() { 
}

MultiTermOrQueryExecutor::~MultiTermOrQueryExecutor() { 
}

std::string MultiTermOrQueryExecutor::toString() const {
    return "MultiTermOr" + MultiQueryExecutor::toString();
}
END_HA3_NAMESPACE(search);

