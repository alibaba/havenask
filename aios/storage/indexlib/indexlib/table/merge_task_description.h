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
#ifndef __INDEXLIB_MERGE_TASK_DESCRIPTION_H
#define __INDEXLIB_MERGE_TASK_DESCRIPTION_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace table {

class MergeTaskDescription : public autil::legacy::Jsonizable
{
public:
    MergeTaskDescription();
    ~MergeTaskDescription();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string name;
    uint32_t cost;
    util::KeyValueMap parameters;

private:
    static std::string MERGE_TASK_NAME_CONFIG;
    static std::string MERGE_TASK_COST_CONFIG;
    static std::string MERGE_TASK_PARAM_CONFIG;

private:
    IE_LOG_DECLARE();
};

typedef std::vector<MergeTaskDescription> MergeTaskDescriptions;

DEFINE_SHARED_PTR(MergeTaskDescription);
}} // namespace indexlib::table

#endif //__INDEXLIB_MERGE_TASK_DESCRIPTION_H
