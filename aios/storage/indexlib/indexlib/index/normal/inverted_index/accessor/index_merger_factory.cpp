/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/normal/inverted_index/accessor/index_merger_factory.h"

#include <sstream>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/expack_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/number/number_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/pack_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/string/string_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/text/text_index_merger.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_merger.h"
#include "indexlib/index/normal/primarykey/primary_key_index_merger_typed.h"
#include "indexlib/index/normal/trie/trie_index_merger.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil::legacy;

DECLARE_REFERENCE_CLASS(plugin, PluginManager);

using namespace indexlib::common;

using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexMergerFactory);

IndexMergerFactory::IndexMergerFactory() { Init(); }

IndexMergerFactory::~IndexMergerFactory() {}

void IndexMergerFactory::Init()
{
    RegisterCreator(IndexMergerCreatorPtr(new legacy::PackIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::TextIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::SpatialIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::DateIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::RangeIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::ExpackIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::StringIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new TrieIndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new PrimaryKey64IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new PrimaryKey128IndexMerger::Creator()));

    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberInt8IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberUInt8IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberInt16IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberUInt16IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberInt32IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberUInt32IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberInt64IndexMerger::Creator()));
    RegisterCreator(IndexMergerCreatorPtr(new legacy::NumberUInt64IndexMerger::Creator()));
}

void IndexMergerFactory::RegisterCreator(IndexMergerCreatorPtr creator)
{
    assert(creator != NULL);

    autil::ScopedLock l(mLock);
    InvertedIndexType type = creator->GetInvertedIndexType();
    IndexCreatorMap::iterator it = mIndexMap.find(type);
    if (it != mIndexMap.end()) {
        mIndexMap.erase(it);
    }
    mIndexMap.insert(std::make_pair(type, creator));
}

IndexMerger* IndexMergerFactory::CreateIndexMerger(InvertedIndexType type,
                                                   const plugin::PluginManagerPtr& pluginManager)
{
    autil::ScopedLock l(mLock);

    if (type == it_customized) {
        return new CustomizedIndexMerger(pluginManager);
    }

    IndexCreatorMap::const_iterator it = mIndexMap.find(type);
    if (it != mIndexMap.end()) {
        return it->second->Create();
    } else {
        stringstream s;
        s << "Index type " << type << " not implemented yet!";
        INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
    }
    return NULL;
}
}} // namespace indexlib::index
