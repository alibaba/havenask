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

#include "indexlib/base/Types.h"
#include "indexlib/framework/IResource.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::framework {

struct TabletDataSchemaGroup : public IResource {
    static const char* NAME;

    std::shared_ptr<config::ITabletSchema> onDiskReadSchema;
    std::shared_ptr<config::ITabletSchema> onDiskWriteSchema;
    std::shared_ptr<config::ITabletSchema> writeSchema;
    std::map<schemaid_t, std::shared_ptr<config::ITabletSchema>> multiVersionSchemas;

    std::shared_ptr<IResource> Clone() override;
    size_t CurrentMemmoryUse() const override { return 0; }
};

} // namespace indexlibv2::framework
