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

#include <mutex>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

class IdGenerator : autil::NoCopyable
{
public:
    explicit IdGenerator(IdMaskType maskType);
    IdGenerator(IdMaskType segMaskType, IdMaskType versionMaskType);
    virtual ~IdGenerator() = default;

    versionid_t GenerateVersionId();
    segmentid_t GetNextSegmentId() const;

    void UpdateBaseVersion(const Version& version);
    Status UpdateBaseVersionId(versionid_t versionId);
    void CommitNextSegmentId();

protected:
    virtual versionid_t GetNextVersionIdUnSafe() const;
    virtual segmentid_t GetNextSegmentIdUnSafe() const;

    versionid_t GetVersionIdMask() const;
    segmentid_t GetSegmentIdMask() const;

protected:
    mutable std::mutex _mtx;
    IdMaskType _segmentMaskType;
    IdMaskType _versionMaskType;
    versionid_t _currentVersionId = INVALID_VERSIONID;
    segmentid_t _currentSegmentId = INVALID_VERSIONID;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
