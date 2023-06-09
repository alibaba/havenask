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
#ifndef __INDEXLIB_VALUE_CONFIG_H
#define __INDEXLIB_VALUE_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, ValueConfigImpl);

namespace indexlib { namespace config {

class ValueConfig : public indexlibv2::config::ValueConfig
{
public:
    ValueConfig();
    ValueConfig(const ValueConfig& other);
    ~ValueConfig();

public:
    void Init(const std::vector<AttributeConfigPtr>& attrConfigs);
    const config::AttributeConfigPtr& GetAttributeConfig(size_t idx) const;

    config::PackAttributeConfigPtr CreatePackAttributeConfig() const;

private:
    ValueConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ValueConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_VALUE_CONFIG_H
