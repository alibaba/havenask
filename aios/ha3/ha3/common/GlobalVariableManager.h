#ifndef ISEARCH_GLOBALVARIABLEMANAGER_H
#define ISEARCH_GLOBALVARIABLEMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/CommonDef.h>
#include <ha3/common/AttributeItem.h>
#include <matchdoc/Trait.h>

BEGIN_HA3_NAMESPACE(common);

class GlobalVariableManager
{
public:
    GlobalVariableManager();
    ~GlobalVariableManager();
private:
    GlobalVariableManager(const GlobalVariableManager &);
    GlobalVariableManager& operator=(const GlobalVariableManager &);
public:
    template<typename T>
    T* declareGlobalVariable(const std::string &variName,
                             bool needSerialize = false);

    template<typename T>
    T* findGlobalVariable(const std::string &variName) const;

    AttributeItemMapPtr getNeedSerializeGlobalVariables() const;

private:
    typedef std::map<std::string, std::pair<bool, AttributeItemPtr> > GlobalVariableMap;
private:
    GlobalVariableMap _globalVariableMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(GlobalVariableManager);
//////////////////////////////////////////////////////////////////
template<typename T>
inline T *GlobalVariableManager::declareGlobalVariable(const std::string &variName,
        bool needSerialize)
{
    if (needSerialize && std::is_same<typename matchdoc::ToStringTypeTraits<T>::Type,
            matchdoc::ToStringType>::value)
    {
        return NULL;
    }
    std::pair<bool, AttributeItemPtr> &v = _globalVariableMap[variName];
    if (v.second != NULL) {
        AttributeItemTyped<T> *p = dynamic_cast<AttributeItemTyped<T> * >(v.second.get());
        if (p) {
            v.first = v.first || needSerialize;
            return p->getPointer();
        } else {
            return NULL;
        }
    } else {
        AttributeItemTyped<T> *p = new AttributeItemTyped<T>(T());
        v.first = needSerialize;
        v.second.reset(p);
        return p->getPointer();
    }
}
template<typename T>
inline T* GlobalVariableManager::findGlobalVariable(const std::string &variName) const {
    GlobalVariableMap::const_iterator it = _globalVariableMap.find(variName);
    if (it == _globalVariableMap.end()) {
        return NULL;
    }
    AttributeItemTyped<T> *p =
        dynamic_cast<AttributeItemTyped<T> * >(it->second.second.get());
    if (!p) {
        return NULL;
    }
    return p->getPointer();
}
END_HA3_NAMESPACE(common);

#endif //ISEARCH_GLOBALVARIABLEMANAGER_H
