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
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlibv2::index {

class PatchFileInfo : public autil::legacy::Jsonizable
{
public:
    PatchFileInfo(segmentid_t _srcSegment, segmentid_t _destSegment,
                  std::shared_ptr<indexlib::file_system::IDirectory> _patchDirectory,
                  const std::string& _patchFileName);

    PatchFileInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    bool operator==(const PatchFileInfo& other) const;

public:
    segmentid_t srcSegment = INVALID_SEGMENTID;
    segmentid_t destSegment = INVALID_SEGMENTID;
    std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
    // patchFullPath includes directory path and file name, and is used for serialization.
    std::string patchFullPath;
    std::string patchFileName;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
