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
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"

namespace indexlibv2::table {

class MergePlan : public framework::IndexTaskResource, public autil::legacy::Jsonizable
{
public:
    MergePlan(std::string name, framework::IndexTaskResourceType type);
    ~MergePlan();

public:
    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    static framework::Version CreateNewVersion(const std::shared_ptr<MergePlan>& MergePlan,
                                               const framework::IndexTaskContext* taskContext);

    void AddMergePlan(const SegmentMergePlan& segmentMergePlan) { _mergePlan.push_back(segmentMergePlan); }

    const SegmentMergePlan& GetSegmentMergePlan(size_t index)
    {
        assert(index < _mergePlan.size());
        return _mergePlan[index];
    }
    SegmentMergePlan* MutableSegmentMergePlan(size_t index)
    {
        assert(index < _mergePlan.size());
        return &_mergePlan[index];
    }

    size_t Size() const { return _mergePlan.size(); }

    const framework::Version& GetTargetVersion() const { return _targetVersion; }
    framework::Version& GetTargetVersion() { return _targetVersion; }

    void SetTargetVersion(framework::Version targetVersion) { _targetVersion = std::move(targetVersion); }

    void AddTaskLogInTargetVersion(const std::string& taskType, const framework::IndexTaskContext* taskContext);

private:
    std::vector<SegmentMergePlan> _mergePlan;
    framework::Version _targetVersion;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
