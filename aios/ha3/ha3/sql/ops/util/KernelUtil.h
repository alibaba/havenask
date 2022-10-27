#ifndef ISEARCH_KERNELUTIL_H
#define ISEARCH_KERNELUTIL_H

#include <ha3/sql/common/common.h>
#include <autil/mem_pool/Pool.h>
#include <navi/engine/KernelInitContext.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(sql);

#define KERNEL_REQUIRES(ptr, msg)               \
    if ((ptr) == nullptr) {                     \
        SQL_LOG(ERROR, msg);                    \
        return navi::EC_ABORT;                  \
    }

class KernelUtil
{
public:
    KernelUtil() {}
    ~KernelUtil() {}
public:
    static autil::mem_pool::Pool *getPool(navi::KernelInitContext &context);
    static void stripName(std::string &name);
    static void stripName(std::vector<std::string> &vec);
    template<typename T>
    static void stripName(std::map<std::string, T> &kv);
    template <typename T>
    static void stripName(T &value);

    static bool toJsonString(autil::legacy::Jsonizable::JsonWrapper &wrapper,
                             const std::string &fieldName, std::string &outVal);
    static bool toJsonWrapper(autil::legacy::Jsonizable::JsonWrapper &wrapper,
                              const std::string &fieldName,
                              autil::legacy::Jsonizable::JsonWrapper &outWrapper);

public:
    static const std::string FIELD_PREIFX;
private:
    HA3_LOG_DECLARE();
};

template<typename T>
void KernelUtil::stripName(std::map<std::string, T> &kv) {
    std::map<std::string, T> newMap = {};
    for (auto &pair : kv) {
        std::string key = pair.first;
        stripName(key);
        T &value = pair.second;
        stripName(value);
        newMap.insert(std::make_pair(key, std::move(value)));
    }
    kv = std::move(newMap);
}

template<typename T>
void KernelUtil::stripName(T &value) {
}

template <typename T, bool = matchdoc::ConstructTypeTraits<T>::NeedConstruct()>
struct InitializeIfNeeded {
    inline void operator()(T &val) {}
};

template <typename T>
struct InitializeIfNeeded<T, false> {
    inline void operator()(T &val) {
        val = 0;
    }
};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_KERNELUTIL_H
