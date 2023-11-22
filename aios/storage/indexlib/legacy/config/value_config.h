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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class ValueConfigImpl;
typedef std::shared_ptr<ValueConfigImpl> ValueConfigImplPtr;
} // namespace indexlib::config

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ValueConfig> ValueConfigPtr;
}} // namespace indexlib::config
