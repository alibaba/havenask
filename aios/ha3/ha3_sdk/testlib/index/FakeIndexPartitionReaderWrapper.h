#pragma once

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"

namespace isearch {
namespace search {

class FakeIndexPartitionReaderWrapper : public IndexPartitionReaderWrapper {
public:
    FakeIndexPartitionReaderWrapper(const std::map<std::string, uint32_t> *indexName2IdMap,
                                    const std::map<std::string, uint32_t> *attrName2IdMap,
                                    const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
                                    bool ownPointer);
    ~FakeIndexPartitionReaderWrapper();

private:
    FakeIndexPartitionReaderWrapper(const FakeIndexPartitionReaderWrapper &);
    FakeIndexPartitionReaderWrapper &operator=(const FakeIndexPartitionReaderWrapper &);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeIndexPartitionReaderWrapper> FakeIndexPartitionReaderWrapperPtr;

} // namespace search
} // namespace isearch
