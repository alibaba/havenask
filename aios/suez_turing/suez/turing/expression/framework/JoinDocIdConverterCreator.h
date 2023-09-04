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

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/framework/JoinDocIdConverterBase.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class JoinInfo;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

namespace suez {
namespace turing {

class JoinDocIdConverterCreator {
public:
    typedef std::map<std::string, JoinDocIdConverterBase *> ConverterMapType;

public:
    JoinDocIdConverterCreator(autil::mem_pool::Pool *pool, matchdoc::MatchDocAllocator *matchDocAllocator);
    virtual ~JoinDocIdConverterCreator();

private:
    JoinDocIdConverterCreator(const JoinDocIdConverterCreator &);
    JoinDocIdConverterCreator &operator=(const JoinDocIdConverterCreator &);

public:
    virtual JoinDocIdConverterBase *createJoinDocIdConverter(const indexlib::partition::JoinInfo &joinInfo);

    const ConverterMapType &getConverterMap() const { return _converterMap; }
    bool isEmpty() const { return _converterMap.empty(); }
    bool hasStrongJoinConverter() const { return _hasStrongJoinConverter; }

public:
    // for test
    void addConverter(const std::string &name, JoinDocIdConverterBase *converter) {
        _converterMap[name] = converter;
        if (converter->isStrongJoin()) {
            _hasStrongJoinConverter = true;
        }
    }

protected:
    ConverterMapType _converterMap;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
    autil::mem_pool::Pool *_pool;
    bool _hasStrongJoinConverter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<JoinDocIdConverterCreator> JoinDocIdConverterCreatorPtr;

} // namespace turing
} // namespace suez
