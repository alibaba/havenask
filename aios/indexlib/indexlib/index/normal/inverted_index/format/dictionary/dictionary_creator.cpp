#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_typed_factory.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/hash_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/hash_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/common_disk_hash_dictionary_reader.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DictionaryCreator);

DictionaryCreator::DictionaryCreator() 
{
}

DictionaryCreator::~DictionaryCreator() 
{
}

DictionaryReader* DictionaryCreator::CreateReader(const IndexConfigPtr& config)
{
    if (config->IsHashTypedDictionary())
    {
        return DictionaryTypedFactory<DictionaryReader, HashDictionaryReaderTyped>
            ::GetInstance()->Create(config->GetIndexType());
    }
    return DictionaryTypedFactory<DictionaryReader, TieredDictionaryReaderTyped>
        ::GetInstance()->Create(config->GetIndexType());
}

DictionaryReader* DictionaryCreator::CreateIntegrateReader(const IndexConfigPtr& config)
{
    if (config->IsHashTypedDictionary())
    {
        return DictionaryTypedFactory<DictionaryReader, IntegrateHashDictionaryReaderTyped>
            ::GetInstance()->Create(config->GetIndexType());
    }
    return DictionaryTypedFactory<DictionaryReader, IntegrateTieredDictionaryReaderTyped>
        ::GetInstance()->Create(config->GetIndexType());
}

DictionaryWriter* DictionaryCreator::CreateWriter(
        const IndexConfigPtr& config, PoolBase* pool)
{
    if (config->IsHashTypedDictionary())
    {
        return DictionaryTypedFactory<DictionaryWriter, HashDictionaryWriter>
            ::GetInstance()->Create(config->GetIndexType(), pool);
    }
    return DictionaryTypedFactory<DictionaryWriter, TieredDictionaryWriter>
        ::GetInstance()->Create(config->GetIndexType(), pool);
}

DictionaryReader* DictionaryCreator::CreateDiskReader(const IndexConfigPtr& config)
{
    if (config->IsHashTypedDictionary())
    {
        return DictionaryTypedFactory<DictionaryReader, CommonDiskHashDictionaryReaderTyped>
            ::GetInstance()->Create(config->GetIndexType());
    }
    return DictionaryTypedFactory<DictionaryReader, CommonDiskTieredDictionaryReaderTyped>
        ::GetInstance()->Create(config->GetIndexType());
}

IE_NAMESPACE_END(index);

