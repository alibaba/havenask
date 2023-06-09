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
#include "indexlib/document/raw_document/KeyMapManager.h"

using namespace std;
using namespace autil;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, KeyMapManager);

KeyMapManager::KeyMapManager() : KeyMapManager(4096) {}

KeyMapManager::KeyMapManager(size_t maxKeySize)
    : _hashMapPrimary(new KeyMap(maxKeySize))
    , _fastUpdateable(false)
    , _maxKeySize(maxKeySize)
{
}

std::shared_ptr<KeyMap> KeyMapManager::getHashMapPrimary()
{
    ScopedLock sl(_mutex);
    _fastUpdateable = false;
    return _hashMapPrimary;
}

void KeyMapManager::updatePrimary(std::shared_ptr<KeyMap>& increment)
{
    if (increment == NULL || increment->size() == 0) {
        return;
    }

    ScopedLock sl(_mutex);
    if (!_hashMapPrimary) {
        return;
    }

    if (_hashMapPrimary->size() > _maxKeySize) {
        AUTIL_LOG(INFO, "Primary Hash Map cleared, size [%lu] over %lu", _hashMapPrimary->size(), _maxKeySize);
        _hashMapPrimary.reset();
        return;
    }

    if (_fastUpdateable == false) {
        _hashMapPrimary.reset(_hashMapPrimary->clone());
    }
    _hashMapPrimary->merge(*increment);
    _fastUpdateable = true;
}
}} // namespace indexlibv2::document
