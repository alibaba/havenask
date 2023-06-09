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
#ifndef __INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H
#define __INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H

#include <memory>

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, FieldConfig);

namespace indexlib { namespace common {

class DefaultAttributeValueInitializerCreator : public AttributeValueInitializerCreator
{
public:
    DefaultAttributeValueInitializerCreator(const config::AttributeConfigPtr& attrConfig);

    ~DefaultAttributeValueInitializerCreator();

public:
    // for reader (global data)
    AttributeValueInitializerPtr Create(const index_base::PartitionDataPtr& partitionData) override;

    // for segment build (local segment data)
    AttributeValueInitializerPtr Create(const index_base::InMemorySegmentReaderPtr& inMemSegmentReader) override;

public:
    DefaultAttributeValueInitializerPtr Create();

private:
    config::FieldConfigPtr mFieldConfig;
    AttributeConvertorPtr mConvertor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultAttributeValueInitializerCreator);
}} // namespace indexlib::common

#endif //__INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H
