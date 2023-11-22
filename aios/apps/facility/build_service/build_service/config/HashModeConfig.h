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
#include <string>
#include <utility>

#include "build_service/common_define.h"
#include "build_service/config/HashMode.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/config/ITabletSchema.h"

namespace build_service { namespace config {

class HashModeConfig
{
public:
    HashModeConfig();
    ~HashModeConfig();

private:
    HashModeConfig(const HashModeConfig&);
    HashModeConfig& operator=(const HashModeConfig&);

public:
    bool init(const std::string& clusterName, ResourceReader* resourceReader,
              const std::shared_ptr<indexlibv2::config::ITabletSchema>& schemaPtr);
    bool getHashMode(const std::string& regionName, HashMode& hashMode) const
    {
        const auto iter = _hashModeMap.find(regionName);
        if (iter == _hashModeMap.end()) {
            return false;
        }
        hashMode = iter->second;
        return true;
    }
    const std::string& getClusterName() const { return _clusterName; }

private:
    typedef std::map<std::string, HashMode> HashModeMap;

private:
    bool initHashModeInfo(const std::string& clusterName, ResourceReader* resourceReader,
                          const std::shared_ptr<indexlibv2::config::ITabletSchema>& schemaPtr,
                          HashModeMap& regionNameToHashMode, HashMode& defaultHashMode, bool& hasDefaultHashMode);
    bool insertHashNode(const HashModeMap& regionNameToHashMode, const std::string& regionName, bool hasDefaultHashMode,
                        HashMode defaultHashMode);

private:
    const std::shared_ptr<indexlibv2::config::ITabletSchema> _schemaPtr;
    const ResourceReader* _resourceReader;
    HashModeMap _hashModeMap;
    std::string _clusterName;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(HashModeConfig);

}} // namespace build_service::config
