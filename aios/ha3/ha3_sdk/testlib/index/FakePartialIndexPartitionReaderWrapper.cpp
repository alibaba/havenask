#include "ha3_sdk/testlib/index/FakePartialIndexPartitionReaderWrapper.h"

#include <iosfwd>

#include "autil/Log.h"
#include "ha3/search/PartialIndexPartitionReaderWrapper.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FakePartialIndexPartitionReaderWrapper);

FakePartialIndexPartitionReaderWrapper::FakePartialIndexPartitionReaderWrapper(
    const std::map<std::string, uint32_t> *indexName2IdMap,
    const std::map<std::string, uint32_t> *attrName2IdMap,
    const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
    bool ownPointer)
    : PartialIndexPartitionReaderWrapper(
        indexName2IdMap, attrName2IdMap, indexReaderVec, ownPointer) {}

FakePartialIndexPartitionReaderWrapper::~FakePartialIndexPartitionReaderWrapper() {}

} // namespace search
} // namespace isearch
