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
#include <mutex>
#include <vector>

#include "autil/Log.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::framework {
struct VersionDeployDescription;

class TabletReaderContainer final
{
public:
    using ITabletReaderPtr = std::shared_ptr<ITabletReader>;

    TabletReaderContainer(const std::string& tabletName) : _tabletName(tabletName) {}
    ~TabletReaderContainer() = default;

    //如果versionDeployDescription是nullptr
    //这个reader的version对应的所有segment都会被完整保留，
    //否则会参考description来决定哪些文件需要保留
    void AddTabletReader(const std::shared_ptr<TabletData>& tabletData, const ITabletReaderPtr& tabletReader,
                         const std::shared_ptr<VersionDeployDescription>& versionDeployDescription);
    bool HasTabletReader(versionid_t versionId) const;
    void GetIncVersions(std::vector<Version>& versions) const;
    versionid_t GetLatestReaderVersionId() const;
    bool IsUsingSegment(segmentid_t segmentId) const;
    ITabletReaderPtr GetOldestTabletReader() const;
    ITabletReaderPtr GetLatestTabletReader() const;
    void EvictOldestTabletReader();
    void Close();
    size_t Size() const;
    versionid_t GetOldestIncVersionId() const;
    versionid_t GetLatestIncVersionId() const;
    int64_t GetLatestIncVersionTimestamp() const;
    int64_t GetLatestIncVersionTaskLogTimestamp() const;
    std::vector<std::shared_ptr<TabletData>> GetTabletDatas() const;
    bool GetNeedKeepFiles(std::set<std::string>* keepFiles);
    void GetNeedKeepSegments(std::set<segmentid_t>* keepSegments);
    versionid_t GetLatestPrivateVersion() const;

public:
    ITabletReaderPtr TEST_GetTabletReader(versionid_t versionId) const;

private:
    mutable std::mutex _mutex;
    const std::string _tabletName;
    std::vector<std::tuple<std::shared_ptr<TabletData>, ITabletReaderPtr, std::shared_ptr<VersionDeployDescription>>>
        _tabletReaderVec;
    versionid_t _latestPrivateVersion = INVALID_VERSIONID;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
