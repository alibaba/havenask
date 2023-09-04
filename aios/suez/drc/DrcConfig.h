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
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/HashMode.h"

namespace suez {

struct SourceConfig final : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    void addParam(const std::string &key, const std::string &value) { parameters[key] = value; }

    std::string type;
    std::map<std::string, std::string> parameters;
};

struct SinkConfig final : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    std::string name;
    std::string type;
    int64_t sourceStartOffset = 0;
    build_service::config::HashMode distConfig;
    std::map<std::string, std::string> parameters;
};

class DrcConfig final : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    void setEnabled(bool flag) { _enabled = flag; }
    bool isEnabled() const { return _enabled; }
    const std::string &getCheckpointRoot() const { return _checkpointRoot; }
    void setCheckpointRoot(std::string checkpointRoot) { _checkpointRoot = std::move(checkpointRoot); }
    const SourceConfig &getSourceConfig() const { return _sourceConfig; }
    SourceConfig &getSourceConfig() { return _sourceConfig; }
    const std::vector<SinkConfig> &getSinkConfigs() const { return _sinkConfigs; }
    int64_t getLogLoopIntervalMs() const { return _logLoopIntervalMs; }
    int64_t getStartOffset() const { return _startOffset; }
    bool shareMode() const { return _shareMode; }
    DrcConfig deriveForSingleSink(const SinkConfig &sink) const;

private:
    bool _enabled = false;
    std::string _checkpointRoot;
    SourceConfig _sourceConfig;
    std::vector<SinkConfig> _sinkConfigs;
    int64_t _logLoopIntervalMs = 2;
    int64_t _startOffset = -1;
    // shareMode: 不同sink是否共用checkpoint
    bool _shareMode = true;
};

} // namespace suez
