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
#ifndef ISEARCH_EXPRESSION_POOLEDOBJECT_H
#define ISEARCH_EXPRESSION_POOLEDOBJECT_H

#include "expression/common.h"
#include <stack>
#include "autil/Lock.h"

namespace expression {

template <class ObjectType>
class PooledObject : public ObjectType
{
public:
    class ObjectPool {
    public:
        ObjectPool() {
            _objectOutsideCount = 0;
        }
        ~ObjectPool() {
            assert(_objectOutsideCount == 0);
            while (!_pool.empty()) {
                ObjectType* object = _pool.top();
                object->destroy();
                _pool.pop();
            }
        }
    public:
        ObjectType* allocate(ObjectType* orgObject) {
            {
                autil::ScopedLock lock(_mutex);
                ++_objectOutsideCount;
                if (!_pool.empty()) {
                    ObjectType* ret = _pool.top();
                    _pool.pop();
                    return ret;
                }
            }
            ObjectType *object = orgObject->clone();
            PooledObject<ObjectType>* pooledObject =
                static_cast<PooledObject<ObjectType>* >(object);
            pooledObject->setOwner(this);
            return object;
        }
        void deallocate(ObjectType* object) {
            autil::ScopedLock lock(_mutex);
            _pool.push(object);
            --_objectOutsideCount;
        }
    private:
        uint32_t _objectOutsideCount;
        std::stack<ObjectType*> _pool;
        autil::ThreadMutex _mutex;
    };
public:
    PooledObject();
    PooledObject(const PooledObject &other);
    ~PooledObject();
public:
    /* override */ ObjectType* allocate();
    /* override */ void deallocate();
    virtual void reset() {
    }
public:
    void setOwner(ObjectPool *objectPool) {
        _objectPool = objectPool;
    }
private:
    ObjectPool *_objectPool;
    bool _ownPool;
private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////

template <class ObjectType>
PooledObject<ObjectType>::PooledObject() {
    _objectPool = new ObjectPool();
    _ownPool = true;
}

template <class ObjectType>
PooledObject<ObjectType>::PooledObject(const PooledObject &other) 
    : ObjectType(other)
{
    _objectPool = NULL;
    _ownPool = false;
}

template <class ObjectType>
PooledObject<ObjectType>::~PooledObject() {
    if (_ownPool) {
        delete _objectPool;
    }
    _objectPool = NULL;
}

template <class ObjectType>
ObjectType *PooledObject<ObjectType>::allocate() {
    return _objectPool->allocate(this);
}

template <class ObjectType>
void PooledObject<ObjectType>::deallocate() {
    if (!_objectPool || _ownPool) {
        return ObjectType::deallocate();
    }
    reset();
    return _objectPool->deallocate(this);
}

}

#endif //ISEARCH_EXPRESSION_POOLEDOBJECT_H
