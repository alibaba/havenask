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
#include "indexlib/index/attribute/AttributeBuildWorkItem.h"

namespace indexlib::index {
template <typename DiskIndexerType, typename MemIndexerType>
class AttributeBuildWorkItem;
}

namespace indexlibv2::table {
class VirtualAttributeDiskIndexer;
class VirtualAttributeMemIndexer;
} // namespace indexlibv2::table

namespace indexlib::table {
class SingleVirtualAttributeBuilder;

class VirtualAttributeBuildWorkItem
    : public index::AttributeBuildWorkItem<indexlibv2::table::VirtualAttributeDiskIndexer,
                                           indexlibv2::table::VirtualAttributeMemIndexer>
{
public:
    VirtualAttributeBuildWorkItem(SingleVirtualAttributeBuilder* builder,
                                  indexlibv2::document::IDocumentBatch* documentBatch);
    ~VirtualAttributeBuildWorkItem();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
