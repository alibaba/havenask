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
#ifndef __INDEXLIB_ATTRIBUTE_UPDATER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_UPDATER_FACTORY_H

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_creator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace index {

class AttributeUpdaterFactory : public util::Singleton<AttributeUpdaterFactory>
{
public:
    typedef std::map<FieldType, AttributeUpdaterCreatorPtr> CreatorMap;

protected:
    AttributeUpdaterFactory();
    friend class util::LazyInstantiation;

public:
    ~AttributeUpdaterFactory();

public:
    AttributeUpdater* CreateAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                             const config::AttributeConfigPtr& attrConfig);

    void RegisterCreator(AttributeUpdaterCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeUpdaterCreatorPtr creator);

    bool IsAttributeUpdatable(const config::AttributeConfigPtr& attrConfig);

protected:
#ifdef ATTRIBUTE_UPDATER_FACTORY_UNITTEST
    friend class AttributeUpdaterFactoryTest;
    CreatorMap& GetMap() { return mUpdaterCreators; }
#endif

private:
    void Init();
    AttributeUpdater* CreateAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                             const config::AttributeConfigPtr& attrConfig, const CreatorMap& creators);

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mUpdaterCreators;
    CreatorMap mMultiValueUpdaterCreators;

    IE_LOG_DECLARE();
};

typedef std::shared_ptr<AttributeUpdaterFactory> AttributeUpdaterFactoryPtr;
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_UPDATER_FACTORY_H
