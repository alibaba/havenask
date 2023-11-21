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

#include <stddef.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/custom_merger/CustomMergePlan.h"
#include "build_service/util/Log.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace build_service { namespace custom_merger {

class CustomMergeMeta : public autil::legacy::Jsonizable
{
public:
    CustomMergeMeta();
    ~CustomMergeMeta();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void addMergePlan(const CustomMergePlan& mergePlan) { _mergePlans.push_back(mergePlan); }
    void store(const std::string& path, indexlib::file_system::FenceContext* fenceContext);
    void load(const std::string& path);
    bool getMergePlan(size_t instanceId, CustomMergePlan& plan) const;

private:
    std::vector<CustomMergePlan> _mergePlans;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergeMeta);

}} // namespace build_service::custom_merger
