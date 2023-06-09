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
#include "indexlib/framework/IdGenerator.h"

#include "indexlib/base/Types.h"
#include "indexlib/framework/Segment.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IdGenerator);

IdGenerator::IdGenerator(IdMaskType maskType) : _segmentMaskType(maskType), _versionMaskType(maskType) {}

IdGenerator::IdGenerator(IdMaskType segmentMaskType, IdMaskType versionMaskType)
    : _segmentMaskType(segmentMaskType)
    , _versionMaskType(versionMaskType)
{
}

Status IdGenerator::UpdateBaseVersionId(versionid_t versionId)
{
    std::lock_guard<std::mutex> lock(_mtx);
    if (versionId & GetVersionIdMask()) {
        if (versionId <= _currentVersionId) {
            AUTIL_LOG(ERROR, "version id[%d] <= currentVersionId[%d]", versionId, _currentVersionId);
            return Status::Exist();
        } else {
            _currentVersionId = versionId;
            return Status::OK();
        }
    }
    AUTIL_LOG(ERROR, "invalid version id [%d] does not match [%d]", versionId, GetVersionIdMask());
    return Status::InvalidArgs();
}

void IdGenerator::UpdateBaseVersion(const Version& version)
{
    if (!version.IsValid()) {
        return;
    }
    std::lock_guard<std::mutex> lock(_mtx);
    versionid_t versionIdMask = GetVersionIdMask();
    segmentid_t segmentIdMask = GetSegmentIdMask();
    if (version.GetVersionId() & versionIdMask) {
        _currentVersionId = std::max(_currentVersionId, version.GetVersionId());
    }
    if (version.GetLastSegmentId() & segmentIdMask) {
        _currentSegmentId = std::max(_currentSegmentId, version.GetLastSegmentId());
    }
}

versionid_t IdGenerator::GetVersionIdMask() const
{
    switch (_versionMaskType) {
    case IdMaskType::BUILD_PUBLIC:
        return Version::PUBLIC_VERSION_ID_MASK;
    case IdMaskType::BUILD_PRIVATE:
        return Version::PRIVATE_VERSION_ID_MASK;
    default:
        assert(false);
    }
    return INVALID_VERSIONID;
}

segmentid_t IdGenerator::GetSegmentIdMask() const
{
    switch (_segmentMaskType) {
    case IdMaskType::BUILD_PUBLIC:
        return Segment::PUBLIC_SEGMENT_ID_MASK;
    case IdMaskType::BUILD_PRIVATE:
        return Segment::PRIVATE_SEGMENT_ID_MASK;
    default:
        assert(false);
    }
    return INVALID_SEGMENTID;
}

versionid_t IdGenerator::GenerateVersionId()
{
    std::lock_guard<std::mutex> lock(_mtx);
    _currentVersionId = GetNextVersionIdUnSafe();
    return _currentVersionId;
}

versionid_t IdGenerator::GetNextVersionIdUnSafe() const
{
    return (_currentVersionId == INVALID_VERSIONID) ? GetVersionIdMask() : _currentVersionId + 1;
}

segmentid_t IdGenerator::GetNextSegmentId() const
{
    std::lock_guard<std::mutex> lock(_mtx);
    return GetNextSegmentIdUnSafe();
}

segmentid_t IdGenerator::GetNextSegmentIdUnSafe() const
{
    return (_currentSegmentId == INVALID_SEGMENTID) ? GetSegmentIdMask() : _currentSegmentId + 1;
}

void IdGenerator::CommitNextSegmentId()
{
    std::lock_guard<std::mutex> lock(_mtx);
    _currentSegmentId = GetNextSegmentIdUnSafe();
}

} // namespace indexlibv2::framework
