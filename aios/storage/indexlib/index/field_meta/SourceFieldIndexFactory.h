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
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/field_meta/ISourceFieldReader.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlibv2::index {
class IIndexMerger;
struct DiskIndexerParameter;
} // namespace indexlibv2::index

namespace indexlib::index {

class SourceFieldIndexFactory : private autil::NoCopyable
{
public:
    SourceFieldIndexFactory();
    ~SourceFieldIndexFactory();

public:
    static std::shared_ptr<indexlibv2::index::IIndexMerger>
    CreateSourceFieldMerger(const std::shared_ptr<FieldMetaConfig>& config);
    static std::shared_ptr<ISourceFieldReader>
    CreateSourceFieldReader(FieldMetaConfig::MetaSourceType sourceType,
                            const indexlibv2::index::DiskIndexerParameter& indexerParam);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
