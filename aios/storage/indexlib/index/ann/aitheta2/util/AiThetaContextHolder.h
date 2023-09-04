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

#include "autil/Lock.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
namespace indexlibv2::index::ann {

class AiThetaContextHolder
{
public:
    AiThetaContextHolder() {}
    ~AiThetaContextHolder() { Clear(); }

public:
    template <typename T>
    std::pair<bool, AiThetaContext*> CreateIfNotExist(const T& searcher, const std::string& searcherName,
                                                      const std::string& searchParams)
    {
        std::string id = autil::StringUtil::toString(pthread_self()).append(searcherName).append(searchParams);
        bool isNew = true;
        {
            autil::ScopedReadLock lock(_readWriteLock);
            auto iterator = _indexContextMap.find(id);
            if (iterator != _indexContextMap.end()) {
                isNew = false;
                return {isNew, iterator->second};
            }
        }
        AiThetaContext* ctx = searcher->create_context().release();
        autil::ScopedWriteLock lock(_readWriteLock);
        _indexContextMap.emplace(id, ctx);
        return {isNew, ctx};
    }

private:
    void Clear()
    {
        autil::ScopedWriteLock lock(_readWriteLock);
        for (auto& [_, ctx] : _indexContextMap) {
            if (ctx != nullptr) {
                delete ctx;
                ctx = nullptr;
            }
        }
        _indexContextMap.clear();
    }

private:
    mutable autil::ReadWriteLock _readWriteLock;
    std::unordered_map<std::string, AiThetaContext*> _indexContextMap;
};

typedef std::shared_ptr<AiThetaContextHolder> AiThetaContextHolderPtr;

} // namespace indexlibv2::index::ann
