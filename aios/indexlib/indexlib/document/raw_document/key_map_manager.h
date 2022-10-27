#ifndef __INDEXLIB_KEYMAP_MANAGER_H
#define __INDEXLIB_KEYMAP_MANAGER_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/raw_document/key_map.h"
#include <autil/Lock.h>

IE_NAMESPACE_BEGIN(document);

class KeyMapManager
{
public:
    KeyMapManager(size_t maxKeySize = 1000);
private:
    KeyMapManager(const KeyMapManager &);
    KeyMapManager& operator=(const KeyMapManager &);
public:
    KeyMapPtr getHashMapPrimary();
    void updatePrimary(KeyMapPtr &);

private:
    KeyMapPtr _hashMapPrimary;
    // if primary has been updated & yet not assign to any doc
    // then it can be fast updated with no copy. 
    bool _fastUpdateable;
    autil::ThreadMutex _mutex;
    size_t _maxKeySize;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyMapManager);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KEYMAP_MANAGER_H
