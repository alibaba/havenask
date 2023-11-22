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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class ConfigReaderAccessor : public autil::legacy::Jsonizable
{
public:
    ConfigReaderAccessor(const std::string& dataTableName);
    ~ConfigReaderAccessor();

    struct ClusterInfo : public autil::legacy::Jsonizable {
        std::string clusterName;
        int64_t schemaId;
        ClusterInfo() : schemaId(-1) {}
        ClusterInfo(std::string clusterNameInput, int64_t schemaIdInput)
        {
            clusterName = clusterNameInput;
            schemaId = schemaIdInput;
        }
        bool operator<(const ClusterInfo& other) const
        {
            if (schemaId != other.schemaId) {
                return schemaId < other.schemaId;
            }
            return clusterName < other.clusterName;
        }
        bool operator==(const ClusterInfo& other) const
        {
            return clusterName == other.clusterName ? schemaId == other.schemaId : false;
        }
        void Jsonize(Jsonizable::JsonWrapper& json)
        {
            json.Jsonize("clusterName", clusterName, clusterName);
            json.Jsonize("schemaId", schemaId, schemaId);
        }
    };

private:
    ConfigReaderAccessor(const ConfigReaderAccessor&);
    ConfigReaderAccessor& operator=(const ConfigReaderAccessor&);

public:
    std::string getLatestConfigPath();
    bool addConfig(const ResourceReaderPtr& resourceReader, bool needClusterInfo);
    ResourceReaderPtr getLatestConfig();
    ResourceReaderPtr getConfig(const std::string& configPath);
    ResourceReaderPtr getConfig(const std::string& clusterName, const int64_t schemaId);

    void deleteConfig(const std::string& clusterName, const int64_t schemaId);
    static bool getClusterInfos(const ResourceReaderPtr& resourceReader, std::string dataTableName,
                                std::vector<ClusterInfo>& clusterInfos);
    int64_t getMinSchemaId(const std::string& clusterName);
    int64_t getMaxSchemaId(const std::string& clusterName);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void getAllClusterInfos(std::vector<ClusterInfo>& allClusterInfos);
    bool getAllClusterNames(std::vector<std::string>& clusterNames)
    {
        auto reader = getLatestConfig();
        return reader->getAllClusters(_dataTableName, clusterNames);
    }

private:
    void clearUselessConfig();

private:
    typedef std::map<std::string, int64_t> ConfigPathToResourceReaderMap;
    typedef std::map<ClusterInfo, int64_t> ClusterInfoToResourceReaderMap;
    std::string _dataTableName;
    std::string _latestConfigPath;
    std::vector<ResourceReaderPtr> _resourceReaders;
    ConfigPathToResourceReaderMap _configToResourceReader;
    ClusterInfoToResourceReaderMap _clusterInfoToResourceReader;
    mutable autil::RecursiveThreadMutex _lock;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ConfigReaderAccessor);

}} // namespace build_service::config
