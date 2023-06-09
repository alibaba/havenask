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
#include <deque>
#include <functional>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>


namespace isearch {
namespace sql {
constexpr int32_t DefaultSlotSize = 200;
constexpr int32_t DefaultPoolSize = 200;

class ObjectPool {
private:
    struct ObjectSlot {
        int32_t maxSize {DefaultSlotSize};

        virtual ~ObjectSlot() = default;
    };

    template <class T>
    struct ObjectSlotTyped : public ObjectSlot {
        std::deque<T*> deq;

        ~ObjectSlotTyped() {
            for (auto p : deq) {
                delete p;
            }
        }
    };

    using ObjectSlotPtr = std::shared_ptr<ObjectSlot>;
    using Pair = std::pair<std::string, ObjectSlotPtr>;
    using PairList = std::list<Pair>;
    using IterMap = std::unordered_map<std::string, std::list<Pair>::iterator>;

public:
    ObjectPool() {}
    ObjectPool(int32_t _maxSize)
        : maxSize(_maxSize) {}
    ~ObjectPool() {
    }

public:
    template <typename T>
    std::shared_ptr<T> get(const std::string &key, const std::function<T *(void)> &f) {
        auto fullkey = genKey<T>(key);
        T *p = getPooledObject<T>(fullkey);
        p = p ? p : f();
        return std::shared_ptr<T>(p, [fullkey = std::move(fullkey), this](T *t) {
            this->put(fullkey, t);
        });
    }

    template <typename T>
    int getKeySize(const std::string &key) {
        auto fullkey = genKey<T>(key);
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = iterMap.find(fullkey);
        if (iter == iterMap.end()) {
            return 0;
        }
        auto slot = getSlot<T>(iter);
        if (slot == nullptr) {
            return 0;
        }
        return slot->deq.size();
        return 0;
    }

    template <typename T>
    void reverse(const std::string key, int size) {
        auto fullkey = genKey<T>(key);
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = iterMap.find(fullkey);
        if (iter != iterMap.end()) {
            auto slot = getSlot<T>(iter);
            if (slot != nullptr) {
                slot->maxSize = size;
            }
        }
    }

private:
    template <class T>
    ObjectSlotTyped<T>* getSlot(IterMap::iterator iter) {
        auto slot = iter->second->second.get();
        return dynamic_cast<ObjectSlotTyped<T>*>(slot);
    }

    template <class T>
    T* getPooledObject(const std::string &key) {
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = iterMap.find(key);
        if (iter != iterMap.end()) {
            auto slot = getSlot<T>(iter);
            if (slot == nullptr) {
                return nullptr;
            }
            objList.splice(objList.begin(), objList, iter->second);
            auto &deq = slot->deq;
            if (!deq.empty()) {
                auto p = deq.back();
                deq.pop_back();
                return p;
            }
        }
        return nullptr;
    }

    template <class T>
    bool putPooledObject(const std::string& key, T *p, ObjectSlotPtr &deleteSlot) {
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = iterMap.find(key);
        if (iter != iterMap.end()) {
            auto slot = getSlot<T>(iter);
            if (slot == nullptr) {
                return false;
            }
            auto &deq = slot->deq;
            if (deq.size() < slot->maxSize) {
                deq.emplace_back(p);
                objList.splice(objList.begin(), objList, iter->second);
                return true;
            } else {
                return false;
            }
        } else {
            auto slot = std::make_shared<ObjectSlotTyped<T>>();
            slot->deq.emplace_back(p);
            objList.emplace_front(key, std::move(slot));
            if (objList.size() > maxSize) {
                auto last = std::prev(objList.end());
                deleteSlot.swap(last->second);
                iterMap.erase(last->first);
                objList.pop_back();
            }
            iterMap.emplace(key, objList.begin());
            return true;
        }
        return false;
    }

    template <class T>
    void put(std::string key, T *p) {
        p->clearObject();
        ObjectSlotPtr deleteSlot;
        if (!putPooledObject(key, p, deleteSlot)) {
            delete p;
        }
    }

    template <typename T>
    std::string genKey(const std::string &key) {
        return std::string(typeid(T).name()).append(key);
    }
private:
    IterMap iterMap;
    PairList objList;
    int32_t maxSize {DefaultPoolSize};
    std::mutex mtx;
};
typedef std::shared_ptr<ObjectPool> ObjectPoolPtr;

class ObjectPoolReadOnly {
    class ObjectSlot {
    public:
        ObjectSlot(void *data, std::function<void(void *)> destructFunc)
            : _data(data)
            , _destructFunc(destructFunc) {}

        ~ObjectSlot() {
            _destructFunc(_data);
        }

    public:
        void *_data;
        std::function<void(void *)> _destructFunc;
    };

    using ObjectSlotPtr = std::shared_ptr<ObjectSlot>;
    using Pair = std::pair<std::string, ObjectSlotPtr>;
    using PairList = std::list<Pair>;
    using IterMap = std::unordered_map<std::string, std::list<Pair>::iterator>;

public:
    ObjectPoolReadOnly() {}
    ObjectPoolReadOnly(int32_t _maxSize)
        : maxSize(_maxSize) {}
    ~ObjectPoolReadOnly() {}

public:
    template <typename T>
    T *get(const std::string &key, std::function<T *(void)> createFunc) {
        auto fullkey = genKey<T>(key);
        T *p = nullptr;
        {
            std::lock_guard<std::mutex> lock(mtx);
            auto iter = iterMap.find(fullkey);
            if (iter != iterMap.end()) {
                p = static_cast<T *>(iter->second->second->_data);
                objList.splice(objList.begin(), objList, iter->second);
                if (p)
                    return p;
            }
        }

        // not found
        p = createFunc();
        if (p) {
            put<T>(fullkey, p);
        }
        return p;
    }

    template <typename T>
    int getKeySize(const std::string &key) {
        auto fullkey = genKey<T>(key);
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = iterMap.find(fullkey);
        if (iter != iterMap.end()) {
            return 1;
        }
        return 0;
    }

private:
    template <typename T>
    void put(const std::string &key, T *p) {
        ObjectSlotPtr deleteSlot;
        std::string deleteKey;
        auto slot = std::make_shared<ObjectSlot>((void *)p, [](void *data) { delete (T *)data; });
        {
            std::lock_guard<std::mutex> lock(mtx);
            objList.emplace_front(key, std::move(slot));
            if (objList.size() > maxSize) {
                auto last = std::prev(objList.end());
                deleteSlot.swap(last->second);
                deleteKey.swap(last->first);
                objList.pop_back();
            }

            iterMap.emplace(key, objList.begin());
            if (!deleteKey.empty()) {
                iterMap.erase(deleteKey);
            }
        }

        deleteSlot.reset();
    }

    template <typename T>
    std::string genKey(const std::string &key) {
        return std::string(typeid(T).name()).append(key);
    }

private:
    IterMap iterMap;
    PairList objList;
    int32_t maxSize {DefaultPoolSize};
    std::mutex mtx;
};

typedef std::shared_ptr<ObjectPoolReadOnly> ObjectPoolReadOnlyPtr;

} // namespace sql
} // namespace isearch
