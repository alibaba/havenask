#include "ha3/search/test/FakeQueryExecutor.h"

#include <assert.h>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/misc/common.h"

using namespace indexlib::index;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FakeQueryExecutor);

FakeQueryExecutor::FakeQueryExecutor() {}

FakeQueryExecutor::~FakeQueryExecutor() {}
const std::string FakeQueryExecutor::getName() const {
    return "FakeQueryExecutor";
}

docid_t FakeQueryExecutor::getDocId() {
    assert(false);
    return INVALID_DOCID;
}

docid_t FakeQueryExecutor::seek(docid_t id) {
    assert(false);
    return INVALID_DOCID;
}

std::string FakeQueryExecutor::toString() const {
    return "";
}

} // namespace search
} // namespace isearch
