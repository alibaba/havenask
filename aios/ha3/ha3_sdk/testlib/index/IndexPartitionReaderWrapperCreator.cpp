#include <ha3_sdk/testlib/index/IndexPartitionReaderWrapperCreator.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, IndexPartitionReaderWrapperCreator);

IndexPartitionReaderWrapperCreator::IndexPartitionReaderWrapperCreator() { 
}

IndexPartitionReaderWrapperCreator::~IndexPartitionReaderWrapperCreator() { 
}

void IndexPartitionReaderWrapperCreator::CreateIndexPartitionReaderWrapper(
        const vector<IE_NAMESPACE(partition)::IndexPartitionPtr>& indexPartitions,
        ReaderWrapperContainer& container)
{
    for (size_t i = 0; i < indexPartitions.size(); i++)
    {
        container.indexPartReaders.push_back(indexPartitions[i]->GetReader());
        auto schema = indexPartitions[i]->GetSchema();
        auto indexSchema = schema->GetIndexSchema();
        for (auto iter = indexSchema->Begin();
             iter != indexSchema->End(); iter++)
        {
            container.indexName2IdMap[(*iter)->GetIndexName()] = i;
        }
        auto attrSchema = schema->GetAttributeSchema();
        for (auto iter = attrSchema->Begin();
             iter != attrSchema->End(); iter++)
        {
            container.attrName2IdMap[(*iter)->GetAttrName()] = i;
        }
    }
    container.readerWrapper.reset(new IndexPartitionReaderWrapper(
                    &container.indexName2IdMap,
                    &container.attrName2IdMap,
                    container.indexPartReaders));
}


END_HA3_NAMESPACE(search);

