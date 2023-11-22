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
#include "autil/NoCopyable.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

class MetaFactory : private autil::NoCopyable
{
public:
    MetaFactory() = default;
    ~MetaFactory() = default;

public:
    static std::pair<Status, std::vector<std::shared_ptr<IFieldMeta>>>
    CreateFieldMetas(const std::shared_ptr<FieldMetaConfig>& fieldMetaConfig);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
