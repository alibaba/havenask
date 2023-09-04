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
#include "build_service/build_task/UpdateLocatorTaskItemV2.h"

#include "build_service/build_task/SingleBuilder.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/VersionLoader.h"

namespace build_service { namespace build_task {

BS_LOG_SETUP(build_task, UpdateLocatorTaskItemV2);

UpdateLocatorTaskItemV2::UpdateLocatorTaskItemV2(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                                 workflow::SwiftProcessedDocProducer* producer, SingleBuilder* builder)
    : _rootDir(rootDir)
    , _producer(producer)
    , _builder(builder)
{
}

UpdateLocatorTaskItemV2::~UpdateLocatorTaskItemV2() { Run(); }

void UpdateLocatorTaskItemV2::Run()
{
    versionid_t versionId = _builder->getOpenedPublishedVersion();
    if (!(versionId & indexlibv2::framework::Version::PUBLIC_VERSION_ID_MASK)) {
        BS_INTERVAL_LOG(60, WARN, "invalid public version id [%d]", versionId);
        return;
    }
    if (versionId == _versionId) {
        return;
    }
    auto [status, version] = indexlibv2::framework::VersionLoader::GetVersion(_rootDir, versionId);
    if (!status.IsOK()) {
        BS_INTERVAL_LOG(60, WARN, "load version [%d] failed ", versionId);
        return;
    }
    if (version == nullptr) {
        assert(false);
        BS_INTERVAL_LOG(60, WARN, "version is nullptr");
        return;
    }
    const indexlibv2::framework::Locator& locator = version->GetLocator();
    indexlibv2::base::Progress::Offset offset = locator.GetOffset();
    if (locator == _locator || offset == indexlibv2::base::Progress::INVALID_OFFSET) {
        return;
    }

    if (!_producer->updateCommittedCheckpoint(offset)) {
        BS_INTERVAL_LOG(60, WARN, "update committed checkpoint failed, locator[%s], version id[%d]",
                        locator.DebugString().c_str(), versionId);
    } else {
        _locator = locator;
        _versionId = versionId;
    }
}

}} // namespace build_service::build_task
