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

#include "autil/WorkItem.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class DumpItem
{
public:
    DumpItem() {}
    virtual ~DumpItem() {}

public:
    virtual void process(autil::mem_pool::PoolBase* pool) = 0;
    virtual void destroy() = 0;
    virtual void drop() = 0;
};
}} // namespace indexlib::common
