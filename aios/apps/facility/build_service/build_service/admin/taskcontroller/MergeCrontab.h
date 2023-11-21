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
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/OfflineIndexConfigMap.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MergeTrigger;

class MergeCrontab : public autil::legacy::Jsonizable
{
public:
    MergeCrontab();
    ~MergeCrontab();

private:
    MergeCrontab(const MergeCrontab&);
    MergeCrontab& operator=(const MergeCrontab&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    bool start(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName);
    std::vector<std::string> generateMergeTasks() const;

public:
    bool operator==(const MergeCrontab& other) const;

private:
    std::map<std::string, std::string> getPeriodicMergeTasks(const config::OfflineIndexConfigMap& configs) const;

private:
    typedef std::vector<MergeTrigger*> MergeTriggers;
    MergeTriggers _mergeTriggers;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergeCrontab);

}} // namespace build_service::admin
