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
#include "indexlib/legacy/indexlib.h"

namespace indexlib::common {
class AttributeValueInitializerCreator;
typedef std::shared_ptr<AttributeValueInitializerCreator> AttributeValueInitializerCreatorPtr;
} // namespace indexlib::common

namespace indexlib { namespace config {

class VirtualAttributeConfigCreator
{
private:
    VirtualAttributeConfigCreator();
    ~VirtualAttributeConfigCreator();

public:
    static AttributeConfigPtr
    Create(const std::string& name, FieldType type, bool multiValue, const std::string& defaultValue,
           const common::AttributeValueInitializerCreatorPtr& creator = common::AttributeValueInitializerCreatorPtr());

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<VirtualAttributeConfigCreator> VirtualAttributeConfigCreatorPtr;
}} // namespace indexlib::config
