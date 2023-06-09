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
#ifndef NAVI_DOMAINHOLDER_H
#define NAVI_DOMAINHOLDER_H

#include "navi/engine/GraphDomain.h"
#include "autil/Lock.h"

namespace navi {

template <typename T>
class DomainHolder
{
public:
    DomainHolder()
        : _terminated(false)
        , _acquireCount(0)
    {
    }
    ~DomainHolder() {
        assert(_acquireCount == 0);
        assert(!_domain);
    }
private:
    DomainHolder(const DomainHolder &);
    DomainHolder &operator=(const DomainHolder &);
public:
    bool init(const std::shared_ptr<T> &domain);
    void terminate();
    bool acquire();
    bool release();
    std::shared_ptr<T> getDomain() const;
private:
    mutable autil::ReadWriteLock _domainLock;
    bool _terminated;
    int32_t _acquireCount;
    std::shared_ptr<T> _domain;
};

template <typename T>
bool DomainHolder<T>::init(const std::shared_ptr<T> &domain) {
    if (!domain) {
        return false;
    }
    {
        autil::ScopedWriteLock lock(_domainLock);
        if (_domain || _terminated) {
            return false;
        }
        _domain = domain;
    }
    return acquire();
}

template <typename T>
void DomainHolder<T>::terminate() {
    {
        autil::ScopedWriteLock lock(_domainLock);
        _terminated = true;
    }
    release();
}

template <typename T>
bool DomainHolder<T>::acquire() {
    autil::ScopedWriteLock lock(_domainLock);
    if (!_domain || _terminated) {
        return false;
    }
    auto ret = _domain->acquire(GDHR_DOMAIN_HOLDER);
    if (ret) {
        _acquireCount++;
    }
    return ret;
}

template <typename T>
bool DomainHolder<T>::release() {
    autil::ScopedWriteLock lock(_domainLock);
    if (_domain && _acquireCount > 0) {
        _acquireCount--;
        auto worker = _domain->release(GDHR_DOMAIN_HOLDER);
        if (worker) {
            worker->decItemCount();
        }
        if (0 == _acquireCount) {
            _domain.reset();
        }
        return true;
    } else {
        return false;
    }
}

template <typename T>
std::shared_ptr<T> DomainHolder<T>::getDomain() const {
    autil::ScopedReadLock lock(_domainLock);
    return _domain;
}

template <typename T>
class DomainHolderScope
{
public:
    DomainHolderScope(DomainHolder<T> &domainHolder)
        : _domainHolder(domainHolder)
    {
        _success = _domainHolder.acquire();
    }
    ~DomainHolderScope() {
        if (_success) {
            _domainHolder.release();
        }
    }
private:
    DomainHolderScope(const DomainHolderScope &);
    DomainHolderScope &operator=(const DomainHolderScope &);
public:
    std::shared_ptr<T> getDomain() const {
        if (_success) {
            return _domainHolder.getDomain();
        } else {
            return nullptr;
        }
    }
private:
    DomainHolder<T> &_domainHolder;
    bool _success;
};

}

#endif //NAVI_DOMAINHOLDER_H
