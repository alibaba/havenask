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
#include <set>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class CheckpointAccessor : public autil::legacy::Jsonizable
{
public:
    CheckpointAccessor() = default;
    ~CheckpointAccessor() = default;

    CheckpointAccessor(const CheckpointAccessor&) = delete;
    CheckpointAccessor& operator=(const CheckpointAccessor&) = delete;

    void addCheckpoint(const std::string& id, const std::string& checkpointName, const std::string& checkpointStr);
    bool updateCheckpoint(const std::string& id, const std::string& checkpointName, const std::string& checkpointStr);
    void addOrUpdateCheckpoint(const std::string& id, const std::string& checkpointName,
                               const std::string& checkpointStr);

    void cleanCheckpoint(const std::string& id, const std::string& checkpointName);

    void cleanCheckpoints(const std::string& id, const std::set<std::string>& checkpointName);

    void listCheckpoints(const std::string& id, std::vector<std::pair<std::string, std::string>>& checkpoints);

    bool getCheckpoint(const std::string& id, const std::string& checkpointName, std::string& checkpointValue,
                       bool printLog = true);

    bool getLatestCheckpoint(const std::string& id, std::string& checkpointName, std::string& checkpointValue);

    void updateReservedCheckpoint(const std::string& id, int32_t keepCheckpointNum,
                                  std::vector<std::pair<std::string, std::string>>& removeCheckpoints);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    bool createSavepoint(const std::string& applyRole, const std::string& id, const std::string& checkpointName,
                         std::string& errorMsg, const std::string& comment = "");

    bool removeSavepoint(const std::string& applyRole, const std::string& id, const std::string& checkpointName,
                         std::string& errorMsg);

    bool isSavepoint(const std::string& id, const std::string& checkpointName, std::string* comment) const;

private:
    bool findCheckpointUnsafe(const std::string& id, const std::string& checkpointName, std::string& checkpoint);

    bool writeStatus(const std::string& statusStr);

private:
    // checkpoint id -> [ <checkpoint name -> checkpoint value> ...]
    typedef std::map<std::string, std::vector<std::pair<std::string, std::string>>> CheckPointMap;
    typedef std::pair<std::string, std::string> SavepointKey;
    typedef std::map<SavepointKey, std::vector<std::string>> SavepointMap;
    typedef std::map<SavepointKey, std::vector<std::string>> SavepointCommentMap;

    CheckPointMap _checkpoints;
    SavepointMap _savepoints;
    SavepointCommentMap _savepointComments;

    mutable autil::RecursiveThreadMutex _mutex;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CheckpointAccessor);

}} // namespace build_service::common
