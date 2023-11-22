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

#include <stack>

#include "autil/Lock.h"
#include "build_service/util/Log.h"

namespace build_service { namespace plugin {

template <class ObjectType, class ObjectTypeBase = ObjectType>
class PooledObject : public ObjectType
{
public:
    class ObjectPool
    {
    public:
        ObjectPool() { _objectOutsideCount = 0; }
        ~ObjectPool()
        {
            assert(_objectOutsideCount == 0);
            while (!_pool.empty()) {
                ObjectType* object = _pool.top();
                object->destroy();
                _pool.pop();
            }
        }

    public:
        ObjectTypeBase* allocate(ObjectType* orgObject)
        {
            {
                autil::ScopedLock lock(_mutex);
                ++_objectOutsideCount;
                if (!_pool.empty()) {
                    ObjectType* ret = _pool.top();
                    _pool.pop();
                    return ret;
                }
            }
            ObjectTypeBase* object = orgObject->clone();
            PooledObject<ObjectType, ObjectTypeBase>* pooledObject =
                static_cast<PooledObject<ObjectType, ObjectTypeBase>*>(object);
            pooledObject->setOwner(this);
            return object;
        }
        void deallocate(ObjectType* object)
        {
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
    PooledObject(const PooledObject& other);
    ~PooledObject();

public:
    ObjectTypeBase* allocate() override;
    void deallocate() override;
    virtual void reset() {}

public:
    void setOwner(ObjectPool* objectPool) { _objectPool = objectPool; }

private:
    ObjectPool* _objectPool;
    bool _ownPool;

private:
    BS_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////

template <class ObjectType, class ObjectTypeBase>
PooledObject<ObjectType, ObjectTypeBase>::PooledObject()
{
    _objectPool = new ObjectPool();
    _ownPool = true;
}

template <class ObjectType, class ObjectTypeBase>
PooledObject<ObjectType, ObjectTypeBase>::PooledObject(const PooledObject& other) : ObjectType(other)
{
    _objectPool = NULL;
    _ownPool = false;
}

template <class ObjectType, class ObjectTypeBase>
PooledObject<ObjectType, ObjectTypeBase>::~PooledObject()
{
    if (_ownPool) {
        delete _objectPool;
    }
    _objectPool = NULL;
}

template <class ObjectType, class ObjectTypeBase>
ObjectTypeBase* PooledObject<ObjectType, ObjectTypeBase>::allocate()
{
    return _objectPool->allocate(this);
}

template <class ObjectType, class ObjectTypeBase>
void PooledObject<ObjectType, ObjectTypeBase>::deallocate()
{
    if (!_objectPool || _ownPool) {
        return ObjectType::deallocate();
    }
    reset();
    return _objectPool->deallocate(this);
}

}} // namespace build_service::plugin
