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

#include "aios/network/gig/multi_call/interface/RequestGenerator.h"

namespace multi_call {

class CachedRequestGenerator
{
public:
    CachedRequestGenerator(const RequestGeneratorPtr &generator) : _generator(generator) {
    }

public:
    void generate(PartIdTy partCnt, PartRequestMap &requestMap);
    std::string toString() const;

    inline const std::shared_ptr<RequestGenerator> getGenerator() const {
        return _generator;
    }

    inline void setClearCacheAfterGet(bool v) {
        _clearCacheAfterGet = v;
    }

private:
    RequestGeneratorPtr _generator;
    bool _clearCacheAfterGet {false};
    std::map<PartIdTy, PartRequestMap> _cachedPartRequestMap;
};

} // namespace multi_call