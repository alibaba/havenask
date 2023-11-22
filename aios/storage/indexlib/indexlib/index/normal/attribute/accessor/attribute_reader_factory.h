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

#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/index/normal/attribute/accessor/patch_apply_option.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderCreator);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index, JoinDocidAttributeReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace index {

class AttributeReaderFactory : public util::Singleton<AttributeReaderFactory>
{
public:
    typedef std::map<FieldType, AttributeReaderCreatorPtr> CreatorMap;

protected:
    AttributeReaderFactory();
    friend class util::LazyInstantiation;

public:
    ~AttributeReaderFactory();

public:
    AttributeReader* CreateAttributeReader(const config::FieldConfigPtr& fieldConfig,
                                           config::ReadPreference readPreference, AttributeMetrics* metrics);
    void RegisterCreator(AttributeReaderCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeReaderCreatorPtr creator);

    static JoinDocidAttributeReaderPtr CreateJoinAttributeReader(const config::IndexPartitionSchemaPtr& schema,
                                                                 const std::string& attrName,
                                                                 const index_base::PartitionDataPtr& partitionData);

    static AttributeReaderPtr CreateAttributeReader(const config::AttributeConfigPtr& attrConfig,
                                                    const index_base::PartitionDataPtr& partitionData,
                                                    config::ReadPreference readPreference, AttributeMetrics* metrics,
                                                    const AttributeReader* hintReader);

protected:
#ifdef ATTRIBUTE_READER_FACTORY_UNITTEST
    friend class AttributeReaderFactoryTest;
    CreatorMap& GetMap() { return mReaderCreators; }
#endif

private:
    void Init();
    AttributeReader* CreateJoinDocidAttributeReader();
    static JoinDocidAttributeReaderPtr CreateJoinAttributeReader(const config::IndexPartitionSchemaPtr& schema,
                                                                 const std::string& attrName,
                                                                 const index_base::PartitionDataPtr& partitionData,
                                                                 const index_base::PartitionDataPtr& joinPartitionData);
    static PatchApplyStrategy ReadPreferenceToPatchApplyStrategy(config::ReadPreference readPreference);

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mReaderCreators;
    CreatorMap mMultiValueReaderCreators;
};

typedef std::shared_ptr<AttributeReaderFactory> AttributeReaderFactoryPtr;
}} // namespace indexlib::index
