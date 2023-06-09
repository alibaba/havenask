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
#include "indexlib/framework/TabletDataSchemaGroup.h"

namespace indexlibv2::framework {

const char* TabletDataSchemaGroup::NAME = "tablet_data_schema_group";

std::shared_ptr<IResource> TabletDataSchemaGroup::Clone()
{
    auto ret = std::make_shared<TabletDataSchemaGroup>();
    ret->onDiskReadSchema = onDiskReadSchema;
    ret->onDiskWriteSchema = onDiskWriteSchema;
    ret->writeSchema = writeSchema;
    ret->multiVersionSchemas = multiVersionSchemas;
    return ret;
}

} // namespace indexlibv2::framework
