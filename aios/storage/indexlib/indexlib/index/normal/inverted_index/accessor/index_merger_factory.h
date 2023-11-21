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
#pragma once

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, IndexMergerCreator);
DECLARE_REFERENCE_CLASS(index, IndexMerger);

namespace indexlib { namespace index {

class IndexMergerFactory : public util::Singleton<IndexMergerFactory>
{
public:
    typedef std::map<InvertedIndexType, IndexMergerCreatorPtr> IndexCreatorMap;

protected:
    IndexMergerFactory();
    friend class util::LazyInstantiation;

public:
    ~IndexMergerFactory();

public:
    IndexMerger* CreateIndexMerger(InvertedIndexType type, const plugin::PluginManagerPtr& pluginManager);

    void RegisterCreator(IndexMergerCreatorPtr creator);

protected:
    void Init();

protected:
#ifdef INDEX_MERGER_FACTORY_UNITTEST
    friend class IndexMergerFactoryTest;
    IndexCreatorMap& GetMap() { return mIndexMap; }
#endif

private:
    autil::RecursiveThreadMutex mLock;
    IndexCreatorMap mIndexMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMergerFactory);
}} // namespace indexlib::index
