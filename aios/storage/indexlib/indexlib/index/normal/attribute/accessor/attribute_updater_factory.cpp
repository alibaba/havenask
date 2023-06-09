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
#include "indexlib/index/normal/attribute/accessor/attribute_updater_factory.h"

#include <sstream>

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/normal/attribute/accessor/date_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/float_attribute_updater_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/time_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/timestamp_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_updater.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace autil::legacy;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace index {

IE_LOG_SETUP(index, AttributeUpdaterFactory);

AttributeUpdaterFactory::AttributeUpdaterFactory() { Init(); }

AttributeUpdaterFactory::~AttributeUpdaterFactory() {}

void AttributeUpdaterFactory::Init()
{
    RegisterCreator(AttributeUpdaterCreatorPtr(new Int8AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new UInt8AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new Int16AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new UInt16AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new Int32AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new UInt32AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new Int64AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new UInt64AttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new FloatAttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new FloatFp16AttributeUpdaterCreator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new FloatInt8AttributeUpdaterCreator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new DoubleAttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new TimeAttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new DateAttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new TimestampAttributeUpdater::Creator()));
    RegisterCreator(AttributeUpdaterCreatorPtr(new StringAttributeUpdater::Creator()));

    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new Int8MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new UInt8MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new Int16MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new UInt16MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new Int32MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new UInt32MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new Int64MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new UInt64MultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new FloatMultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new DoubleMultiValueAttributeUpdater::Creator()));
    RegisterMultiValueCreator(AttributeUpdaterCreatorPtr(new MultiStringAttributeUpdater::Creator()));
}

void AttributeUpdaterFactory::RegisterCreator(AttributeUpdaterCreatorPtr creator)
{
    assert(creator != NULL);

    ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mUpdaterCreators.find(type);
    if (it != mUpdaterCreators.end()) {
        mUpdaterCreators.erase(it);
    }
    mUpdaterCreators.insert(make_pair(type, creator));
}

void AttributeUpdaterFactory::RegisterMultiValueCreator(AttributeUpdaterCreatorPtr creator)
{
    assert(creator != NULL);

    ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mMultiValueUpdaterCreators.find(type);
    if (it != mMultiValueUpdaterCreators.end()) {
        mMultiValueUpdaterCreators.erase(it);
    }
    mMultiValueUpdaterCreators.insert(make_pair(type, creator));
}

AttributeUpdater* AttributeUpdaterFactory::CreateAttributeUpdater(BuildResourceMetrics* buildResourceMetrics,
                                                                  segmentid_t segId,
                                                                  const AttributeConfigPtr& attrConfig)
{
    if (attrConfig->IsMultiValue()) {
        return CreateAttributeUpdater(buildResourceMetrics, segId, attrConfig, mMultiValueUpdaterCreators);
    } else {
        return CreateAttributeUpdater(buildResourceMetrics, segId, attrConfig, mUpdaterCreators);
    }
}

AttributeUpdater* AttributeUpdaterFactory::CreateAttributeUpdater(BuildResourceMetrics* buildResourceMetrics,
                                                                  segmentid_t segId,
                                                                  const AttributeConfigPtr& attrConfig,
                                                                  const CreatorMap& creators)
{
    ScopedLock l(mLock);

    FieldType type = attrConfig->GetFieldType();
    CreatorMap::const_iterator it = creators.find(type);
    if (it != creators.end()) {
        AttributeUpdater* updater = NULL;
        updater = it->second->Create(buildResourceMetrics, segId, attrConfig);
        return updater;
    } else {
        IE_LOG(WARN, "Unsupported attribute updater for type : %d", type);
        return NULL;
    }
}

bool AttributeUpdaterFactory::IsAttributeUpdatable(const AttributeConfigPtr& attrConfig)
{
    if (!attrConfig->IsMultiValue()) {
        ScopedLock l(mLock);
        CreatorMap::const_iterator it = mUpdaterCreators.find(attrConfig->GetFieldType());
        if (it == mUpdaterCreators.end()) {
            return false;
        }
    }
    if (attrConfig->IsMultiValue()) {
        if (!attrConfig->IsAttributeUpdatable()) {
            return false;
        }
        ScopedLock l(mLock);
        CreatorMap::const_iterator it = mMultiValueUpdaterCreators.find(attrConfig->GetFieldType());
        if (it == mMultiValueUpdaterCreators.end()) {
            return false;
        }
    }

    return true;
}
}} // namespace indexlib::index
