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
#include "indexlib/base/Types.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::config {
class IIndexConfig;
class FieldMetaConfig;
} // namespace indexlibv2::config

namespace indexlib::index {

class FieldMetaMemIndexer;
class FieldMetaConfig;

class SingleFieldMetaBuilder : private autil::NoCopyable
{
public:
    SingleFieldMetaBuilder();
    ~SingleFieldMetaBuilder();

public:
    Status Init(const indexlibv2::framework::TabletData& tabletData,
                const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);

public:
    Status Build(indexlibv2::document::IDocumentBatch* docBatch);

private:
    FieldMetaMemIndexer* _buildingIndexer = nullptr;
    std::shared_ptr<FieldMetaConfig> _fieldMetaConfig = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
