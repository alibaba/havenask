#include "ha3/search/test/IndexPartitionReaderWrapperCreator.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include "autil/Log.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, IndexPartitionReaderWrapperCreator);

IndexPartitionReaderWrapperCreator::IndexPartitionReaderWrapperCreator() {}

IndexPartitionReaderWrapperCreator::~IndexPartitionReaderWrapperCreator() {}

void IndexPartitionReaderWrapperCreator::CreateIndexPartitionReaderWrapper(
    const vector<indexlib::partition::IndexPartitionPtr> &indexPartitions,
    ReaderWrapperContainer &container) {
    for (size_t i = 0; i < indexPartitions.size(); i++) {
        container.indexPartReaders.push_back(indexPartitions[i]->GetReader());
        auto schema = indexPartitions[i]->GetSchema();
        auto indexSchema = schema->GetIndexSchema();
        for (auto iter = indexSchema->Begin(); iter != indexSchema->End(); iter++) {
            container.indexName2IdMap[(*iter)->GetIndexName()] = i;
        }
        auto attrSchema = schema->GetAttributeSchema();
        for (auto iter = attrSchema->Begin(); iter != attrSchema->End(); iter++) {
            container.attrName2IdMap[(*iter)->GetAttrName()] = i;
        }
    }

    container.readerWrapper.reset(new IndexPartitionReaderWrapper(
        &container.indexName2IdMap, &container.attrName2IdMap, &container.indexPartReaders, false));
}

} // namespace search
} // namespace isearch
