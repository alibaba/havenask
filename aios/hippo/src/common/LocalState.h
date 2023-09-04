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
#ifndef HIPPO_LOCALSTATE_H
#define HIPPO_LOCALSTATE_H

#include "util/Log.h"
#include "common/common.h"
#include "common/AppState.h"
#include "autil/WorkItem.h"
#include "autil/ThreadPool.h"

BEGIN_HIPPO_NAMESPACE(common);

class LocalStateReclaimItem : public autil::WorkItem {
public:
    LocalStateReclaimItem(const std::string &basePath, const std::string &fileName)
        : _basePath(basePath)
        , _fileName(fileName)
    {}
    void process();
private:
    std::string _basePath;
    std::string _fileName;
};

class LocalState : public AppState
{
public:
    LocalState(const std::string &basePath);
    ~LocalState();
private:
    LocalState(const LocalState &);
    LocalState& operator=(const LocalState &);
public:
    using AppState::read;
    using AppState::write;

    /* override */ bool read(const std::string &fileName,
                             std::string &content) const;

    /* override */ bool write(const std::string &fileName,
                              const std::string &content);

    /* override */ bool check(const std::string &fileName,
                              bool &bExist);

    /* override */ bool remove(const std::string &path);

private:
    std::string _basePath;
    autil::ThreadPoolPtr _bgReclaimThreadPool;

private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(LocalState);

END_HIPPO_NAMESPACE(common);

#endif //HIPPO_LOCALSTATE_H
