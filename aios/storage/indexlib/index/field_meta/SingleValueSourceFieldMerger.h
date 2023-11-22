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
#include "indexlib/index/attribute/merger/SingleValueAttributeMerger.h"
#include "indexlib/index/field_meta/CommonSourceFieldMerger.h"

namespace indexlib::index {

template <typename T>
class SingleValueSourceFieldMerger : public indexlibv2::index::SingleValueAttributeMerger<T>,
                                     public CommonSourceFieldMerger
{
public:
    SingleValueSourceFieldMerger(const std::shared_ptr<FieldMetaConfig>& config) : CommonSourceFieldMerger(config) {}
    ~SingleValueSourceFieldMerger() {}

private:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;

    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
    GetDiskIndexer(const std::shared_ptr<indexlibv2::framework::Segment>& segment) override;

    std::pair<Status, std::shared_ptr<file_system::IDirectory>>
    GetOutputDirectory(const std::shared_ptr<file_system::IDirectory>& segDir) override;

private:
    std::shared_ptr<FieldMetaConfig> _config;
    std::map<segmentid_t, std::shared_ptr<ISourceFieldReader>> _sourceFieldReaders;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueSourceFieldMerger, T);
////////////////////////////////////////////////////////////////

template <typename T>
std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
SingleValueSourceFieldMerger<T>::GetDiskIndexer(const std::shared_ptr<indexlibv2::framework::Segment>& segment)
{
    return CommonSourceFieldMerger::GetIndexer(segment);
}

template <typename T>
std::pair<Status, std::shared_ptr<file_system::IDirectory>>
SingleValueSourceFieldMerger<T>::GetOutputDirectory(const std::shared_ptr<file_system::IDirectory>& segDir)
{
    return CommonSourceFieldMerger::GetOutputDir(segDir);
}

template <typename T>
Status SingleValueSourceFieldMerger<T>::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                             const std::map<std::string, std::any>& params)
{
    auto status = indexlibv2::index::SingleValueAttributeMerger<T>::Init(indexConfig, params);
    RETURN_IF_STATUS_ERROR(status, "source field merger init failed for index [%s]",
                           indexConfig->GetIndexName().c_str());
    return CommonSourceFieldMerger::Init(indexConfig, params);
}

} // namespace indexlib::index
