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

#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/CustomMergerFactory.h"
#include "build_service/util/Log.h"

namespace build_service { namespace custom_merger {

class BuildinCustomMergerFactory : public CustomMergerFactory
{
public:
    BuildinCustomMergerFactory();
    ~BuildinCustomMergerFactory();

private:
    BuildinCustomMergerFactory(const BuildinCustomMergerFactory&);
    BuildinCustomMergerFactory& operator=(const BuildinCustomMergerFactory&);

public:
    bool init(const KeyValueMap& parameters) override { return true; }
    void destroy() override { delete this; }
    CustomMerger* createCustomMerger(const std::string& mergerName, uint32_t backupId = 0,
                                     const std::string& epochId = "") override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildinCustomMergerFactory);

}} // namespace build_service::custom_merger
