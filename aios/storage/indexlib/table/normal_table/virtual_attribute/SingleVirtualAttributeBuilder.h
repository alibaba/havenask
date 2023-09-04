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
#include "indexlib/base/Status.h"
#include "indexlib/index/attribute/SingleAttributeBuilder.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeDiskIndexer.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeMemIndexer.h"

namespace indexlibv2::config {
class ITabletSchema;
class IIndexConfig;
} // namespace indexlibv2::config
namespace indexlibv2::document {
class IDocument;
}
namespace indexlibv2::framework {
class TabletData;
}
// namespace indexlibv2::table {
// class VirtualAttributeDiskIndexer;
// class VirtualAttributeMemIndexer;
// } // namespace indexlibv2::table

namespace indexlib::table {
class SingleVirtualAttributeBuilder
    : public index::SingleAttributeBuilder<indexlibv2::table::VirtualAttributeDiskIndexer,
                                           indexlibv2::table::VirtualAttributeMemIndexer>
{
public:
    SingleVirtualAttributeBuilder(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);
    ~SingleVirtualAttributeBuilder();

private:
    Status InitConfigRelated(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
