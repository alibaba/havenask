#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/pack_index_merger.h"
#include "indexlib/index/normal/primarykey/primary_key_index_merger_typed.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/expack_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/string/string_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/text/text_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/number/number_index_merger.h"
#include "indexlib/index/normal/trie/trie_index_merger.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_merger.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/misc/exception.h"

#include <sstream>

using namespace std;
using namespace autil::legacy;

DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexMergerFactory);

IndexMergerFactory::IndexMergerFactory() 
{
    Init();
}

IndexMergerFactory::~IndexMergerFactory() 
{
}

void IndexMergerFactory::Init()
{
    RegisterCreator(IndexMergerCreatorPtr(new PackIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new TextIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new SpatialIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new DateIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new RangeIndexMerger::Creator()));    
    RegisterCreator(IndexMergerCreatorPtr(new ExpackIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new StringIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new TrieIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new PrimaryKey64IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new PrimaryKey128IndexMerger::Creator()));

    RegisterCreator(IndexMergerCreatorPtr(
                     new NumberInt8IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberUInt8IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberInt16IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberUInt16IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberInt32IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberUInt32IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberInt64IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(
                    new NumberUInt64IndexMerger::Creator()));
}

void IndexMergerFactory::RegisterCreator(IndexMergerCreatorPtr creator)
{
    assert (creator != NULL);

    autil::ScopedLock l(mLock);
    IndexType type = creator->GetIndexType();
    IndexCreatorMap::iterator it = mIndexMap.find(type);
    if(it != mIndexMap.end())
    {
        mIndexMap.erase(it);
    }
    mIndexMap.insert(std::make_pair(type, creator));
}

IndexMerger* IndexMergerFactory::CreateIndexMerger(IndexType type,
        const plugin::PluginManagerPtr& pluginManager)
{
    autil::ScopedLock l(mLock);

    if (type == it_customized)
    {
        return new CustomizedIndexMerger(pluginManager);
    }

    IndexCreatorMap::const_iterator it = mIndexMap.find(type);
    if (it != mIndexMap.end())
    {
        return it->second->Create();
    }
    else
    {
        stringstream s;
        s << "Index type " << type <<" not implemented yet!";
        INDEXLIB_THROW(misc::UnImplementException, "%s", s.str().c_str());
    }
    return NULL;
}


IE_NAMESPACE_END(index);

