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
#include "build_service/admin/taskcontroller/MergeCrontab.h"

#include <assert.h>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/taskcontroller/MergeTrigger.h"
#include "build_service/admin/taskcontroller/PeriodMergeTrigger.h"
#include "build_service/config/OfflineIndexConfigMap.h"
#include "build_service/config/OfflineMergeConfig.h"
#include "build_service/config/ProcessorInfo.h"
#include "build_service/proto/BasicDefs.pb.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;
using namespace build_service::proto;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, MergeCrontab);

MergeCrontab::MergeCrontab() {}

MergeCrontab::~MergeCrontab()
{
    for (size_t i = 0; i < _mergeTriggers.size(); i++) {
        delete _mergeTriggers[i];
    }
    _mergeTriggers.clear();
}

void MergeCrontab::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        vector<Any> anys;
        anys.reserve(_mergeTriggers.size());
        for (size_t i = 0; i < _mergeTriggers.size(); i++) {
            anys.push_back(ToJson(_mergeTriggers[i]));
        }
        json.Jsonize("merge_triggers", anys);
    } else {
        assert(_mergeTriggers.empty());
        vector<Any> jsonVec;
        json.Jsonize("merge_triggers", jsonVec);
        _mergeTriggers.reserve(jsonVec.size());
        for (size_t i = 0; i < jsonVec.size(); i++) {
            // todo: support more MergeTrigger type?
            unique_ptr<PeriodMergeTrigger> mergeTrigger(new PeriodMergeTrigger);
            FromJson(*mergeTrigger, jsonVec[i]);
            _mergeTriggers.push_back(mergeTrigger.release());
        }
    }
}

bool MergeCrontab::start(const config::ResourceReaderPtr& resourceReader, const string& clusterName)
{
    if (_mergeTriggers.size() > 0) {
        BS_LOG(INFO, "merge crontab for cluster[%s] has already started", clusterName.c_str());
        return true;
    }

    string clusterConfig = resourceReader->getClusterConfRelativePath(clusterName);
    OfflineIndexConfigMap configMap;
    if (!resourceReader->getConfigWithJsonPath(clusterConfig, "offline_index_config", configMap)) {
        string errorMsg = "read offline_index_config from " + clusterConfig + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    map<string, string> mergeTasks = getPeriodicMergeTasks(configMap);
    for (map<string, string>::const_iterator it = mergeTasks.begin(); it != mergeTasks.end(); ++it) {
        MergeTrigger* mergeTrigger = MergeTrigger::create(it->first, it->second);
        if (!mergeTrigger) {
            stringstream ss;
            ss << "fail to create merge trigger name[" << it->first << "],description[" << it->second << "]";
            string errorMsg = ss.str();
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
        _mergeTriggers.push_back(mergeTrigger);
        BS_LOG(INFO, "merge trigger [%s] [%s] created", it->first.c_str(), it->second.c_str());
    }

    return true;
}

vector<string> MergeCrontab::generateMergeTasks() const
{
    vector<string> mergeTasks;
    for (size_t i = 0; i < _mergeTriggers.size(); i++) {
        string mergeTask;
        if (_mergeTriggers[i]->triggerMergeTask(mergeTask)) {
            mergeTasks.push_back(mergeTask);
        }
    }
    return mergeTasks;
}

map<string, string> MergeCrontab::getPeriodicMergeTasks(const OfflineIndexConfigMap& configs) const
{
    map<string, string> ret;
    OfflineIndexConfigMap::ConstIterator it = configs.begin();
    for (; it != configs.end(); ++it) {
        const string& taskName = it->first;
        const string& taskDesc = it->second.offlineMergeConfig.periodMergeDescription;
        if (!taskDesc.empty()) {
            ret[taskName] = taskDesc;
        }
    }
    return ret;
}

bool MergeCrontab::operator==(const MergeCrontab& other) const
{
    if (_mergeTriggers.size() != other._mergeTriggers.size()) {
        return false;
    }
    for (size_t i = 0; i < _mergeTriggers.size(); i++) {
        if (!(*_mergeTriggers[i] == *other._mergeTriggers[i])) {
            return false;
        }
    }
    return true;
}

}} // namespace build_service::admin
