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

#include "autil/Log.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::index {
class AttributeConvertor;
class AttributeConfig;
class PackAttributeConfig;

class AttributeConvertorFactory : public indexlib::util::Singleton<AttributeConvertorFactory>
{
public:
    AttributeConvertorFactory() = default;
    ~AttributeConvertorFactory() = default;

    AttributeConvertor* CreatePackAttrConvertor(const std::shared_ptr<PackAttributeConfig>& packAttrConfig);
    AttributeConvertor* CreateAttrConvertor(const std::shared_ptr<AttributeConfig>& field);
    AttributeConvertor* CreateAttrConvertor(const std::shared_ptr<AttributeConfig>& field, bool encodeEmpty);

private:
    AttributeConvertor* CreateSingleAttrConvertor(const std::shared_ptr<AttributeConfig>& attrConfig);
    AttributeConvertor* CreateMultiAttrConvertor(const std::shared_ptr<AttributeConfig>& attrConfig);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
