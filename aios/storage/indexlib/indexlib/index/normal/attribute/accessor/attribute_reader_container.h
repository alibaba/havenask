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
#ifndef __INDEXLIB_ATTRIBUTE_READER_CONTAINER_H
#define __INDEXLIB_ATTRIBUTE_READER_CONTAINER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, MultiFieldAttributeReader);
DECLARE_REFERENCE_CLASS(index, MultiPackAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);

namespace indexlib { namespace index {

class AttributeReaderContainer;
DEFINE_SHARED_PTR(AttributeReaderContainer);

class AttributeReaderContainer
{
public:
    AttributeReaderContainer(const indexlib::config::IndexPartitionSchemaPtr& schema);
    ~AttributeReaderContainer();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, AttributeMetrics* attrMetrics,
              config::ReadPreference readPreference, bool needPackAttributeReaders, int32_t initReaderThreadCount,
              const AttributeReaderContainer* hintContainer);
    void InitAttributeReader(const index_base::PartitionDataPtr& partitionData, config::ReadPreference readPreference,
                             const std::string& field, const AttributeReaderContainer* hintContainer);

    const AttributeReaderPtr& GetAttributeReader(const std::string& field) const;
    const PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const;
    const PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const;

public:
    const MultiFieldAttributeReaderPtr& GetAttrReaders() const { return mAttrReaders; }
    const MultiFieldAttributeReaderPtr& GetVirtualAttrReaders() const { return mVirtualAttrReaders; }
    const MultiPackAttributeReaderPtr& GetPackAttrReaders() const { return mPackAttrReaders; }

private:
    indexlib::config::IndexPartitionSchemaPtr mSchema;
    MultiFieldAttributeReaderPtr mAttrReaders;
    MultiFieldAttributeReaderPtr mVirtualAttrReaders;
    MultiPackAttributeReaderPtr mPackAttrReaders;

public:
    static PackAttributeReaderPtr RET_EMPTY_PACK_ATTR_READER;
    static AttributeReaderPtr RET_EMPTY_ATTR_READER;
    static AttributeReaderContainerPtr RET_EMPTY_ATTR_READER_CONTAINER;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_READER_CONTAINER_H
