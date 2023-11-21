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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace index {

class AttributeWriterFactory : public util::Singleton<AttributeWriterFactory>
{
public:
    typedef std::map<FieldType, AttributeWriterCreatorPtr> CreatorMap;

protected:
    AttributeWriterFactory();
    friend class util::LazyInstantiation;

public:
    ~AttributeWriterFactory();

public:
    AttributeWriter* CreateAttributeWriter(const config::AttributeConfigPtr& attrConfig,
                                           const config::IndexPartitionOptions& options,
                                           util::BuildResourceMetrics* buildResourceMetrics);

    PackAttributeWriter* CreatePackAttributeWriter(const config::PackAttributeConfigPtr& attrConfig,
                                                   const config::IndexPartitionOptions& options,
                                                   util::BuildResourceMetrics* buildResourceMetrics);

    void RegisterCreator(AttributeWriterCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeWriterCreatorPtr creator);

protected:
#ifdef ATTRIBUTE_WRITER_FACTORY_UNITTEST
    friend class AttributeWriterFactoryTest;
    CreatorMap& GetMap() { return mWriterCreators; }
#endif

private:
    void Init();

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mWriterCreators;
    CreatorMap mMultiValueWriterCreators;
};

typedef std::shared_ptr<AttributeWriterFactory> AttributeWriterFactoryPtr;
}} // namespace indexlib::index
