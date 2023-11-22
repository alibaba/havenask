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

#include <any>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexer.h"
#include "indexlib/index/field_meta/ISourceFieldReader.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {

class CommonSourceFieldMerger
{
public:
    CommonSourceFieldMerger(const std::shared_ptr<FieldMetaConfig>& config) : _config(config) {}
    ~CommonSourceFieldMerger() {}

public:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params);

    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
    GetIndexer(const std::shared_ptr<indexlibv2::framework::Segment>& segment);
    std::pair<Status, std::shared_ptr<file_system::IDirectory>>
    GetOutputDir(const std::shared_ptr<file_system::IDirectory>& segDir);

private:
    std::shared_ptr<FieldMetaConfig> _config;
    std::map<segmentid_t, std::shared_ptr<ISourceFieldReader>> _sourceFieldReaders;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
