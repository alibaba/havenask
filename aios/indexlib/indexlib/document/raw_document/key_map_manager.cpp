#include "indexlib/document/raw_document/key_map_manager.h"
#include "indexlib/document/raw_document/key_map.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KeyMapManager);

KeyMapManager::KeyMapManager(size_t maxKeySize)
    : _hashMapPrimary(new KeyMap(maxKeySize))
    , _fastUpdateable(false)
    , _maxKeySize(maxKeySize)
{
}

KeyMapPtr KeyMapManager::getHashMapPrimary() {
    ScopedLock sl(_mutex);
    _fastUpdateable = false;
    return _hashMapPrimary;
}

void KeyMapManager::updatePrimary(KeyMapPtr &increment) {
    if (increment == NULL || increment->size() == 0) {
        return;
    }

    ScopedLock sl(_mutex);
    if (!_hashMapPrimary) {
        return;
    }

    if (_hashMapPrimary->size() > _maxKeySize) {
        IE_LOG(INFO, "Primary Hash Map cleared, size [%lu] over %lu",
               _hashMapPrimary->size(), _maxKeySize);
        _hashMapPrimary.reset();
        return;
    }

    if (_fastUpdateable == false) {
        _hashMapPrimary.reset(_hashMapPrimary->clone());
    }
    _hashMapPrimary->merge(*increment);
    _fastUpdateable = true;
}

IE_NAMESPACE_END(document);
