#pragma once

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartialIndexPartitionReaderWrapper.h"

namespace isearch {
namespace search {

class FakePartialIndexPartitionReaderWrapper : public PartialIndexPartitionReaderWrapper {
public:
    FakePartialIndexPartitionReaderWrapper(
        const std::map<std::string, uint32_t> *indexName2IdMap,
        const std::map<std::string, uint32_t> *attrName2IdMap,
        const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
        bool ownPointer);
    ~FakePartialIndexPartitionReaderWrapper();

private:
    FakePartialIndexPartitionReaderWrapper(const FakePartialIndexPartitionReaderWrapper &);
    FakePartialIndexPartitionReaderWrapper &
    operator=(const FakePartialIndexPartitionReaderWrapper &);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakePartialIndexPartitionReaderWrapper>
    FakePartialIndexPartitionReaderWrapperPtr;

} // namespace search
} // namespace isearch
