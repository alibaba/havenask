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

#include <string>

#include "autil/WorkItem.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service { namespace admin {

class GenerationRecoverWorkItem : public autil::WorkItem
{
public:
    enum RecoverStatus {
        RECOVER_FAILED,
        RECOVER_SUCCESS,
        NO_NEED_RECOVER,
    };

public:
    GenerationRecoverWorkItem(const std::string& zkRoot, const proto::BuildId& buildId,
                              const GenerationKeeperPtr& keeper);
    ~GenerationRecoverWorkItem();

private:
    GenerationRecoverWorkItem(const GenerationRecoverWorkItem&);
    GenerationRecoverWorkItem& operator=(const GenerationRecoverWorkItem&);

public:
    void process() override;
    void destroy() override {}
    void drop() override { _status = RECOVER_FAILED; }
    GenerationKeeperPtr getGenerationKeeper() { return _keeper; }
    RecoverStatus getRecoverStatus() { return _status; }

private:
    bool recoverOneGenerationKeeper(const proto::BuildId& buildId, bool isStopped);

private:
    std::string _zkRoot;
    proto::BuildId _buildId;
    GenerationKeeperPtr _keeper;
    RecoverStatus _status;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationRecoverWorkItem);

}} // namespace build_service::admin
