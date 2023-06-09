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

#include <memory>

#include "autil/Log.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

namespace indexlib { namespace util {

class MemoryReserver
{
public:
    MemoryReserver(const std::string& name, const BlockMemoryQuotaControllerPtr& memController);
    ~MemoryReserver();

public:
    bool Reserve(int64_t quota);
    int64_t GetFreeQuota() const { return _memController->GetFreeQuota(); }

private:
    std::string _name;
    BlockMemoryQuotaControllerPtr _memController;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MemoryReserver> MemoryReserverPtr;
}} // namespace indexlib::util
