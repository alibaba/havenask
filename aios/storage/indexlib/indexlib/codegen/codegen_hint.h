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
#ifndef __INDEXLIB_CODEGEN_HINT_H
#define __INDEXLIB_CODEGEN_HINT_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/DllWrapper.h"

namespace indexlib { namespace codegen {

class CodegenHint
{
public:
    CodegenHint();
    ~CodegenHint();

public:
    void* getFunction(uint64_t id)
    {
        autil::ScopedReadLock lock(_lock);
        for (size_t i = 0; i < _funcCache.size(); i++) {
            if (_funcCache[i].first == id) {
                return _funcCache[i].second;
            }
        }
        return NULL;
    }

    void registFunction(uint64_t id, void* func)
    {
        if (!func) {
            return;
        }
        autil::ScopedWriteLock lock(_lock);
        for (size_t i = 0; i < _funcCache.size(); i++) {
            if (_funcCache[i].first == id) {
                return;
            }
        }
        _funcCache.push_back(std::make_pair(id, func));
    }
    std::string getSourceCodePath();

public:
    plugin::DllWrapperPtr _handler;
    int64_t _unuseTimestamp;
    std::string _codeHash;

private:
    autil::ReadWriteLock _lock;
    std::vector<std::pair<uint64_t, void*>> _funcCache;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CodegenHint);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_CODEGEN_HINT_H
