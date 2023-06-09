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
#ifndef ISEARCH_BS_OFFLINEINDEXCONFIGMAP_H
#define ISEARCH_BS_OFFLINEINDEXCONFIGMAP_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/OfflineMergeConfig.h"
#include "build_service/config/TaskControllerConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/config/build_config.h"

namespace build_service { namespace config {

struct OfflineIndexConfig {
    indexlib::config::BuildConfig buildConfig;
    OfflineMergeConfig offlineMergeConfig;
};

class OfflineIndexConfigMap : public autil::legacy::Jsonizable
{
public:
    OfflineIndexConfigMap();
    ~OfflineIndexConfigMap();

public:
    typedef std::map<std::string, OfflineIndexConfig> InnerMap;
    typedef InnerMap::iterator Iterator;
    typedef InnerMap::const_iterator ConstIterator;

public:
    Iterator begin() { return _offlineIndexConfigMap.begin(); }

    Iterator end() { return _offlineIndexConfigMap.end(); }

    Iterator find(const std::string& configName) { return _offlineIndexConfigMap.find(configName); }

    ConstIterator begin() const { return _offlineIndexConfigMap.begin(); }

    ConstIterator end() const { return _offlineIndexConfigMap.end(); }

    ConstIterator find(const std::string& configName) const { return _offlineIndexConfigMap.find(configName); }

    OfflineIndexConfig& operator[](const std::string& configName) { return _offlineIndexConfigMap[configName]; }

    size_t size() const { return _offlineIndexConfigMap.size(); }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    InnerMap _offlineIndexConfigMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OfflineIndexConfigMap);

}} // namespace build_service::config

#endif // ISEARCH_BS_OFFLINEINDEXCONFIGMAP_H
