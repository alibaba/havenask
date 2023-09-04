#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderWrapper.h"

#include <iosfwd>

#include "autil/Log.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FakeIndexPartitionReaderWrapper);

FakeIndexPartitionReaderWrapper::FakeIndexPartitionReaderWrapper(
    const std::map<std::string, uint32_t> *indexName2IdMap,
    const std::map<std::string, uint32_t> *attrName2IdMap,
    const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
    bool ownPointer)
    : IndexPartitionReaderWrapper(indexName2IdMap, attrName2IdMap, indexReaderVec, ownPointer) {}

FakeIndexPartitionReaderWrapper::~FakeIndexPartitionReaderWrapper() {}

} // namespace search
} // namespace isearch
