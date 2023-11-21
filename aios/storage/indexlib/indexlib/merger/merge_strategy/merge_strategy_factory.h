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

#include <map>
#include <memory>
#include <string>

#include "autil/Lock.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace merger {

class MergeStrategyFactory : public util::Singleton<MergeStrategyFactory>
{
public:
    typedef std::map<std::string, MergeStrategyCreatorPtr> CreatorMap;

protected:
    MergeStrategyFactory();
    friend class util::LazyInstantiation;

public:
    ~MergeStrategyFactory();

public:
    MergeStrategyPtr CreateMergeStrategy(const std::string& identifier, const SegmentDirectoryPtr& segDir,
                                         const config::IndexPartitionSchemaPtr& schema);
    void RegisterCreator(MergeStrategyCreatorPtr creator);

protected:
    void Init();

protected:
#ifdef MERGE_STRATEGY_FACTORY_UNITTEST
    friend class MergeStrategyFactoryTest;
    CreatorMap& GetMap() { return mCreatorMap; }
#endif

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mCreatorMap;
};

typedef std::shared_ptr<MergeStrategyFactory> MergeStrategyFactoryPtr;
}} // namespace indexlib::merger
