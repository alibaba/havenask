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
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"

namespace indexlibv2::framework {
class IndexTaskResourceManager;
class TabletData;
class Segment;
} // namespace indexlibv2::framework
namespace indexlibv2::config {
class ITabletSchema;
}
namespace indexlibv2::index {
template <typename T>
class DocumentEvaluator;
class DocumentEvaluatorMaintainer;
} // namespace indexlibv2::index
namespace indexlibv2::table {

class GroupSelectResource : public framework::IndexTaskResource
{
public:
    GroupSelectResource(std::string name, framework::IndexTaskResourceType type);
    ~GroupSelectResource();
    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    void SetSelectInfo(const std::shared_ptr<std::vector<int32_t>>& selectInfo);
    const std::shared_ptr<std::vector<int32_t>>& GetSelectInfo() const;

public:
    static const framework::IndexTaskResourceType RESOURCE_TYPE;

private:
    AUTIL_LOG_DECLARE();
    std::shared_ptr<std::vector<int32_t>> _selectInfo;
};

class SingleSegmentDocumentGroupSelector : private autil::NoCopyable
{
public:
    SingleSegmentDocumentGroupSelector();
    ~SingleSegmentDocumentGroupSelector();
    Status Init(const std::shared_ptr<framework::Segment>& segment,
                const std::shared_ptr<config::ITabletSchema>& schema);

    Status Init(segmentid_t segmentId, const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager);
    Status Dump(const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager);

    std::pair<Status, int32_t> Select(docid_t inSegmentDocId);

private:
    std::unique_ptr<index::DocumentEvaluatorMaintainer> _evaluatorMatainer;
    std::vector<std::shared_ptr<index::DocumentEvaluator<bool>>> _evaluators;
    std::shared_ptr<std::vector<int32_t>> _selectInfo;
    segmentid_t _segmentId;
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::table
