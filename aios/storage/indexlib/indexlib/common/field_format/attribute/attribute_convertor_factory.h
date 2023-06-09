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
#ifndef __INDEXLIB_ATTRIBUTE_CONVERTOR_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_CONVERTOR_FACTORY_H

#include <memory>

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace common {

class AttributeConvertorFactory : public util::Singleton<AttributeConvertorFactory>
{
public:
    ~AttributeConvertorFactory();

public:
    AttributeConvertor* CreateAttrConvertor(const std::shared_ptr<indexlib::config::AttributeConfig>& attrConfig,
                                            TableType tableType);

    AttributeConvertor* CreatePackAttrConvertor(const config::PackAttributeConfigPtr& packAttrConfig);

    AttributeConvertor* CreateAttrConvertor(const std::shared_ptr<indexlib::config::AttributeConfig>& attrConfig);

private:
    AttributeConvertor* CreateSingleAttrConvertor(const config::AttributeConfigPtr& attrConfig);
    AttributeConvertor* CreateMultiAttrConvertor(const config::AttributeConfigPtr& attrConfig);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeConvertorFactory);
}} // namespace indexlib::common

#endif //__INDEXLIB_ATTRIBUTE_CONVERTOR_FACTORY_H
