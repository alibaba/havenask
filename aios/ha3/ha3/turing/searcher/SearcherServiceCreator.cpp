#include <ha3/turing/searcher/SearcherServiceImpl.h>

namespace suez {
SearchManager * createSearchManager() {
    return new isearch::turing::SearcherServiceImpl();
}
}

