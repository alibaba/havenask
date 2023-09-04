#pragma once

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition.h"

namespace isearch {
namespace search {
struct ReaderWrapperContainer {
    // todo support join field info
    std::map<std::string, uint32_t> indexName2IdMap;
    std::map<std::string, uint32_t> attrName2IdMap;
    std::vector<indexlib::partition::IndexPartitionReaderPtr> indexPartReaders;
    IndexPartitionReaderWrapperPtr readerWrapper;
};

class IndexPartitionReaderWrapperCreator {
public:
    IndexPartitionReaderWrapperCreator();
    ~IndexPartitionReaderWrapperCreator();

private:
    IndexPartitionReaderWrapperCreator(const IndexPartitionReaderWrapperCreator &);
    IndexPartitionReaderWrapperCreator &operator=(const IndexPartitionReaderWrapperCreator &);

public:
    static void CreateIndexPartitionReaderWrapper(
        const std::vector<indexlib::partition::IndexPartitionPtr> &indexPartitions,
        ReaderWrapperContainer &container);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexPartitionReaderWrapperCreator> IndexPartitionReaderWrapperCreatorPtr;

} // namespace search
} // namespace isearch
