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

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

class IResource;

// version resource: will reclaim with tabletData
// segment resource: will reclaim with segment
// inherited resource: will not reclaim, but can be updated
// can not add resource with the same name in version/segment/inherited resource
class ResourceMap : private autil::NoCopyable
{
public:
    ResourceMap() {}

public:
    std::shared_ptr<IResource> GetResource(const std::string& name) const;
    Status AddVersionResource(const std::string& name, const std::shared_ptr<IResource>& resource);

    // 1.segment id should not be invalid.
    // 2.segment resource will be reclaimed when ReclaimSegmentResource(reservedSegments) is called, and segmentId is
    // not in reservedSegments
    Status AddSegmentResource(const std::string& name, segmentid_t segmentId,
                              const std::shared_ptr<IResource>& resource);

    Status AddInheritedResource(const std::string& name, const std::shared_ptr<IResource>& resource);

    void ReclaimSegmentResource(const std::set<segmentid_t>& reservedSegments);
    size_t CurrentMemmoryUse() const;

    // will only clone segment resource and inherited resource
    std::shared_ptr<ResourceMap> Clone() const;

    void TEST_Clear();
    void TEST_RemoveResource(const std::string& name);

private:
    std::shared_ptr<IResource> GetResourceWithoutLock(const std::string& name) const;

private:
    mutable std::mutex _resourceMutex;
    std::map<std::string, std::shared_ptr<IResource>> _versionResource;
    std::map<std::string, std::shared_ptr<IResource>> _inheritedResource;
    std::map<std::string, std::pair<segmentid_t, std::shared_ptr<IResource>>> _segmentResource;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
