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
#include "indexlib/document/raw_document/KeyMap.h"

namespace indexlibv2 { namespace document {

class KeyMapManager
{
public:
    KeyMapManager();
    explicit KeyMapManager(size_t maxKeySize);

private:
    KeyMapManager(const KeyMapManager&);
    KeyMapManager& operator=(const KeyMapManager&);

public:
    std::shared_ptr<KeyMap> getHashMapPrimary();
    void updatePrimary(std::shared_ptr<KeyMap>&);

private:
    std::shared_ptr<KeyMap> _hashMapPrimary;
    // if primary has been updated & yet not assign to any doc
    // then it can be fast updated with no copy.
    bool _fastUpdateable;
    autil::ThreadMutex _mutex;
    size_t _maxKeySize;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
