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

#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class IndexPathUtil
{
public:
    static bool GetSegmentId(const std::string& path, segmentid_t& segmentId) noexcept;
    static bool GetVersionId(const std::string& path, versionid_t& versionId) noexcept;
    static bool GetDeployMetaId(const std::string& path, versionid_t& versionId) noexcept;
    static bool GetPatchMetaId(const std::string& path, versionid_t& versionId) noexcept;
    static bool GetSchemaId(const std::string& path, schemaid_t& schemaId) noexcept;
    static bool GetPatchIndexId(const std::string& path, schemaid_t& schemaId) noexcept;

    static bool IsValidSegmentName(const std::string& path) noexcept;
};
}} // namespace indexlib::index_base
