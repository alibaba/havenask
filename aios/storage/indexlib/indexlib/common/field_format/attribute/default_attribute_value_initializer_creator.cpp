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
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, DefaultAttributeValueInitializerCreator);

DefaultAttributeValueInitializerCreator::DefaultAttributeValueInitializerCreator(const AttributeConfigPtr& attrConfig)
{
    assert(attrConfig);
    mFieldConfig = attrConfig->GetFieldConfig();
    assert(mFieldConfig);
    if (mFieldConfig->GetFieldType() == ft_raw) {
        return;
    }
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    assert(convertor);
    convertor->SetEncodeEmpty(true);
    mConvertor = convertor;
}

DefaultAttributeValueInitializerCreator::~DefaultAttributeValueInitializerCreator() {}

AttributeValueInitializerPtr DefaultAttributeValueInitializerCreator::Create(const PartitionDataPtr& partitionData)
{
    return Create();
}

AttributeValueInitializerPtr
DefaultAttributeValueInitializerCreator::Create(const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    return Create();
}

DefaultAttributeValueInitializerPtr DefaultAttributeValueInitializerCreator::Create()
{
    string defaultValueStr = mFieldConfig->GetDefaultValue();
    if (defaultValueStr.empty() && !mFieldConfig->IsMultiValue()) {
        FieldType ft = mFieldConfig->GetFieldType();
        if (ft == ft_int8 || ft == ft_int16 || ft == ft_int32 || ft == ft_int64 || ft == ft_uint8 || ft == ft_uint16 ||
            ft == ft_uint32 || ft == ft_uint64 || ft == ft_float || ft == ft_fp8 || ft == ft_fp16 || ft == ft_double) {
            IE_LOG(DEBUG, "field [%s] not support empty default value string, use \"0\" instead",
                   mFieldConfig->GetFieldName().c_str());
            defaultValueStr = string("0");
        }
    }
    if (mFieldConfig->GetFieldType() == ft_raw) {
        return DefaultAttributeValueInitializerPtr(new DefaultAttributeValueInitializer(FIELD_DEFAULT_STR_VALUE));
    }

    if (defaultValueStr.empty() && mFieldConfig->IsMultiValue() && mFieldConfig->GetFixedMultiValueCount() != -1) {
        FieldType ft = mFieldConfig->GetFieldType();
        if (ft == ft_int8 || ft == ft_int16 || ft == ft_int32 || ft == ft_int64 || ft == ft_uint8 || ft == ft_uint16 ||
            ft == ft_uint32 || ft == ft_uint64 || ft == ft_float || ft == ft_double) {
            IE_LOG(DEBUG, "field [%s] not support empty default value string, use %d \"0\" instead",
                   mFieldConfig->GetFieldName().c_str(), mFieldConfig->GetFixedMultiValueCount());
            vector<string> tmpVec(mFieldConfig->GetFixedMultiValueCount(), string("0"));
            defaultValueStr = StringUtil::toString(tmpVec, mFieldConfig->GetSeparator());
        }
    }
    string defaultValue = mConvertor->Encode(defaultValueStr);
    return DefaultAttributeValueInitializerPtr(new DefaultAttributeValueInitializer(defaultValue));
}
}} // namespace indexlib::common
