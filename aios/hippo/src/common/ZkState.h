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
#ifndef HIPPO_ZKSTATE_H
#define HIPPO_ZKSTATE_H

#include "util/Log.h"
#include "common/common.h"
#include "common/AppState.h"

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"

BEGIN_HIPPO_NAMESPACE(common);

class ZkState : public AppState
{
public:
    ZkState(cm_basic::ZkWrapper *zkWrapper, const std::string &basePath);
    ~ZkState();
private:
    ZkState(const ZkState &);
    ZkState& operator=(const ZkState &);
public:
    using AppState::read;
    using AppState::write;
    /* override */ bool read(const std::string &fileName,
                             std::string &content) const;
    
    /* override */ bool write(const std::string &fileName,
                              const std::string &content);

    /* override */ bool check(const std::string &fileName, bool &bExist);

    /* override */ bool remove(const std::string &path);
private:
    cm_basic::ZkWrapper *_zkWrapper;
    std::string _basePath;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(ZkState);

END_HIPPO_NAMESPACE(common);

#endif //HIPPO_ZKSTATE_H
