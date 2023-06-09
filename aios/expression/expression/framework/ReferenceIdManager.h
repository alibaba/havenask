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
#ifndef ISEARCH_EXPRESSION_REFERENCEIDMANAGER_H
#define ISEARCH_EXPRESSION_REFERENCEIDMANAGER_H

#include "expression/common.h"

namespace expression {

class ReferenceIdManager
{
public:
    static const uint32_t MAX_REFERENCE_ID = 31;
    static const uint32_t INVALID_REFERENCE_ID = (uint32_t)-1;
public:
    ReferenceIdManager()
        : _nextId(0)
    {}
    ~ReferenceIdManager() {}
private:
    ReferenceIdManager(const ReferenceIdManager &);
    ReferenceIdManager& operator=(const ReferenceIdManager &);
public:
    uint32_t requireReferenceId() {
        if (_nextId > MAX_REFERENCE_ID) {
            return INVALID_REFERENCE_ID;
        }
        return _nextId++;
    }
public:
    static bool isReferenceIdValid(uint32_t id) {
        return id != INVALID_REFERENCE_ID;
    }
private:
    uint32_t _nextId;
    
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_REFERENCEIDMANAGER_H
