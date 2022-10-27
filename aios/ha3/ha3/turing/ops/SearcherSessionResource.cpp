#include <ha3/turing/ops/SearcherSessionResource.h>

using namespace std;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(searcher, SearcherSessionResource);

SearcherSessionResource::SearcherSessionResource(uint32_t count)
    : SessionResource(count)
{ 
}

SearcherSessionResource::~SearcherSessionResource() { 
}

END_HA3_NAMESPACE(searcher);

