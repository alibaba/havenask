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
#ifndef ISEARCH_BS_CHECKPOINT_TOOLS_H
#define ISEARCH_BS_CHECKPOINT_TOOLS_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common/CheckpointAccessor.h"

namespace build_service { namespace admin {

class CheckpointTools
{
public:
    static bool getDisableOpIds(const common::CheckpointAccessorPtr& ckpAccessor, const std::string& clusterName,
                                std::vector<schema_opid_t>& ret, schema_opid_t maxOpId = 0)
    {
        std::string ckpValue;
        if (!ckpAccessor->getCheckpoint(BS_CKP_ID_DISABLE_OPIDS, clusterName, ckpValue, false)) {
            return true;
        }
        try {
            autil::legacy::FromJsonString(ret, ckpValue);
        } catch (const autil::legacy::ExceptionBase& e) {
            return false;
        }
        if (maxOpId == 0) {
            return true;
        }
        std::vector<schema_opid_t> tmp;
        for (auto opId : ret) {
            if (opId <= maxOpId) {
                tmp.push_back(opId);
            }
        }
        ret.swap(tmp);
        return true;
    }
    static void addDisableOpIds(const common::CheckpointAccessorPtr& ckpAccessor, const std::string clusterName,
                                const std::vector<schema_opid_t>& disableOpIds)
    {
        if (disableOpIds.empty()) {
            return;
        }
        std::string ckpValue = autil::legacy::ToJsonString(disableOpIds);
        ckpAccessor->addOrUpdateCheckpoint(BS_CKP_ID_DISABLE_OPIDS, clusterName, ckpValue);
    }
};

}}     // namespace build_service::admin
#endif // ISEARCH_BS_CHECKPOINT_TOOLS_H
