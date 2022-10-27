#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <mutex>
#include <unordered_map>
#include <type_traits>
#include <memory>

BEGIN_HA3_NAMESPACE(sql);

class TvfResourceBase {
public:
    virtual ~TvfResourceBase() = default;
public:
    virtual std::string name() const = 0;
};

class TvfResourceContainer {
public:
    bool put(TvfResourceBase *obj) {
        auto name = obj->name();
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _runtimeObjs.find(name);
        if (it != _runtimeObjs.end()) {
            return false;
        }
        _runtimeObjs[name] = std::unique_ptr<TvfResourceBase>(obj);
        return true;
    }

    template <typename T>
    T *get(const std::string &name) const {
        static_assert(std::is_base_of<TvfResourceBase, T>::value, "type invalid");
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _runtimeObjs.find(name);
        if (it == _runtimeObjs.end()) {
            return nullptr;
        }
        return dynamic_cast<T*>(it->second.get());
    }
private:
    mutable std::mutex _mutex;
    std::unordered_map<std::string, std::unique_ptr<TvfResourceBase> > _runtimeObjs;
};

HA3_TYPEDEF_PTR(TvfResourceContainer);
HA3_TYPEDEF_PTR(TvfResourceBase);
END_HA3_NAMESPACE(sql);
