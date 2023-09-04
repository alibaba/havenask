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
#ifndef HIPPO_LEADERSTATE_H
#define HIPPO_LEADERSTATE_H

#include "util/Log.h"
#include "common/common.h"
#include "common/AppState.h"
#include "worker_framework/LeaderChecker.h"


BEGIN_HIPPO_NAMESPACE(common);

class LeaderState : public AppState
{
public:
    LeaderState(worker_framework::LeaderChecker *leaderChecker,
                cm_basic::ZkWrapper *zkWrapper,
                const std::string &basePath,
                const std::string &backupPath = "");
    
    ~LeaderState();
protected:
    // for inheritance
    LeaderState();
private:
    LeaderState(const LeaderState &);
    LeaderState& operator=(const LeaderState &);
public:
    using AppState::read;
    using AppState::write;
    /* override */ bool read(const std::string &fileName,
                             std::string &content) const;
    
    /* override */ bool write(const std::string &fileName,
                              const std::string &content);

    /* override */ bool check(const std::string &fileName, bool &bExist);

    /* override */ bool remove(const std::string &path);

public:
    void setZkPath(const std::string& path) {
        _zkPath = path;
    }

    std::string getZkPath() const {
        return _zkPath;
    }
    
public:
    // test
    void setRealState(AppState *state) {
        delete _realState;
        _realState = state;
    }
    void setBackupState(AppState *state) {
        delete _backupState;
        _backupState = state;
    }
    
protected:
    worker_framework::LeaderChecker *_leaderChecker;
    AppState *_realState;
    AppState *_backupState;
    std::map<std::string, std::string> _contentCache;
    std::string _zkPath;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(LeaderState);

END_HIPPO_NAMESPACE(common);

#endif //HIPPO_LEADERSTATE_H
