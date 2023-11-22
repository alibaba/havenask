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
#include "indexlib/index/attribute/merger/MultiValueAttributeMerger.h"
#include "indexlib/index/field_meta/CommonSourceFieldMerger.h"
namespace indexlib::index {

template <typename T>
class MultiValueSourceFieldMerger : public indexlibv2::index::MultiValueAttributeMerger<T>,
                                    public CommonSourceFieldMerger
{
public:
    MultiValueSourceFieldMerger(const std::shared_ptr<FieldMetaConfig>& config) : CommonSourceFieldMerger(config) {}
    ~MultiValueSourceFieldMerger() {}

public:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;

    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
    GetIndexerFromSegment(const std::shared_ptr<indexlibv2::framework::Segment>& segment,
                          const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig) override;
    std::pair<Status, std::shared_ptr<file_system::IDirectory>>
    GetOutputDirectory(const std::shared_ptr<file_system::IDirectory>& segDir) override;

private:
    std::shared_ptr<FieldMetaConfig> _config;
    std::map<segmentid_t, std::shared_ptr<ISourceFieldReader>> _sourceFieldReaders;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueSourceFieldMerger, T);
//////////////////////////////////////////////////////////////////////////////////////////
template <typename T>
Status MultiValueSourceFieldMerger<T>::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                            const std::map<std::string, std::any>& params)
{
    auto status = indexlibv2::index::MultiValueAttributeMerger<T>::Init(indexConfig, params);
    RETURN_IF_STATUS_ERROR(status, "source field merger init failed for index [%s]",
                           indexConfig->GetIndexName().c_str());
    return CommonSourceFieldMerger::Init(indexConfig, params);
}

template <typename T>
std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> MultiValueSourceFieldMerger<T>::GetIndexerFromSegment(
    const std::shared_ptr<indexlibv2::framework::Segment>& segment,
    const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig)
{
    return CommonSourceFieldMerger::GetIndexer(segment);
}

template <typename T>
std::pair<Status, std::shared_ptr<file_system::IDirectory>>
MultiValueSourceFieldMerger<T>::GetOutputDirectory(const std::shared_ptr<file_system::IDirectory>& segDir)
{
    return CommonSourceFieldMerger::GetOutputDir(segDir);
}

} // namespace indexlib::index
