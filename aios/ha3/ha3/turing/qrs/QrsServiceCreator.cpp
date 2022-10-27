#include <ha3/turing/qrs/QrsServiceImpl.h>

namespace suez {
SearchManager * createSearchManager() {
    return new isearch::turing::QrsServiceImpl();
}
}

