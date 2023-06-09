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

#include <limits>
#include <memory>

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PatchWorkItem : public autil::WorkItem
{
public:
    // also be used to rank complexity when load patch
    enum PatchWorkItemType {
        PWIT_INDEX = 4,
        PWIT_PACK_ATTR = 3,
        PWIT_VAR_NUM = 2,
        PWIT_FIX_LENGTH = 1,
        PWIT_UNKNOWN = 0
    };

public:
    PatchWorkItem(const std::string& id, size_t patchItemCount, bool isSub, PatchWorkItemType ite_type);
    virtual ~PatchWorkItem() {};

public:
    std::string GetIdentifier() const { return _identifier; }
    size_t GetPatchItemCount() const { return _patchCount; }
    void SetCost(size_t cost) { _cost = cost; }
    size_t GetCost() const { return _cost; };
    uint32_t GetItemType() const { return (uint32_t)_type; }

    virtual bool HasNext() const = 0;
    virtual size_t ProcessNext() = 0;

    void process();

    void SetProcessLimit(size_t limit) { _processLimit = limit; }

    int64_t GetLastProcessTime() const;
    size_t GetLastProcessCount() const;
    bool IsSub() const { return _isSub; }

protected:
    std::string _identifier;
    size_t _patchCount;
    size_t _cost;
    bool _isSub;
    PatchWorkItemType _type;
    size_t _processLimit;
    int64_t _lastProcessTime;
    size_t _lastProcessCount;
    mutable autil::ThreadMutex _lock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchWorkItem);
}} // namespace indexlib::index
