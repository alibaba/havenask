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

#include <cstdint>
#include <optional>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

struct WriterUpdateInfo : public autil::legacy::Jsonizable {
    uint32_t updatedMajorVersion = 0;
    std::vector<std::pair<size_t, uint32_t>> updatedMinorVersions;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const WriterUpdateInfo& rhs) const;
};
class ProcessorWriterVersion : public autil::legacy::Jsonizable
{
public:
    ProcessorWriterVersion() = default;
    ProcessorWriterVersion(uint32_t majorVersion, const std::vector<uint32_t>& minorVersions,
                           const std::vector<std::string>& identifier);
    ~ProcessorWriterVersion() {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::optional<WriterUpdateInfo> update(const proto::ProcessorNodes& processorNodes, uint32_t partitionCount,
                                           uint32_t parallelNum);
    WriterUpdateInfo forceUpdateMajorVersion(size_t workerCount);
    uint32_t getMajorVersion() const;
    uint32_t getMinorVersion(size_t idx) const;
    size_t size() const;
    const std::string& getIdentifier(size_t idx) const;
    std::string serilize() const;
    bool deserilize(const std::string& str);

private:
    uint32_t _partitionCount = 0;
    uint32_t _parallelNum = 0;
    uint32_t _majorVersion = 0;
    std::vector<uint32_t> _minorVersions;
    std::vector<std::string> _identifier;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
