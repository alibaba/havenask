#ifndef ISEARCH_DATAPROVIDER_H
#define ISEARCH_DATAPROVIDER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/CommonDef.h>
#include <ha3/common/AttributeItem.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/GlobalVariableManager.h>
#include <matchdoc/Trait.h>


BEGIN_HA3_NAMESPACE(rank);
class ScoringProvider;
END_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
class AllocatorScope;
END_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(common);

class DataProvider
{
public:
    DataProvider();
    DataProvider(const GlobalVariableManagerPtr &globalVariableManagerPtr);
    ~DataProvider();
    DataProvider(const DataProvider &other);
    DataProvider& operator=(const DataProvider &other);

    template<typename T>
    T* declareGlobalVariable(const std::string &variName,
                             bool needSerialize = false);

    template<typename T>
    T* findGlobalVariable(const std::string &variName) const;

    AttributeItemMapPtr getNeedSerializeGlobalVariables() const;

    bool isSubScope() const {
        return _isSubScope;
    }

    void setSubScope(bool isSubScope) {
        _isSubScope = isSubScope;
    }

    void unsetSubScope() {
        _isSubScope = false;
    }

    void setGlobalVariableManager(const common::GlobalVariableManagerPtr &globalVariableManagerPtr) {
        _globalVariableMgr = globalVariableManagerPtr;
    }

    GlobalVariableManagerPtr getGlobalVariableManager() {
        return _globalVariableMgr;
    }
private:
    typedef std::map<std::string, std::pair<bool, AttributeItemPtr> > GlobalVariableMap;
private:
    //GlobalVariableMap _globalVariableMap;
    GlobalVariableManagerPtr _globalVariableMgr;
    bool _isSubScope;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DataProvider);
//////////////////////////////////////////////////////////////////

template<typename T>
inline T *DataProvider::declareGlobalVariable(const std::string &variName,
        bool needSerialize)
{
    return _globalVariableMgr->declareGlobalVariable<T>(variName, needSerialize);
}
template<typename T>
inline T* DataProvider::findGlobalVariable(const std::string &variName) const {
    return _globalVariableMgr->findGlobalVariable<T>(variName);
}
END_HA3_NAMESPACE(common);

#endif //ISEARCH_DATAPROVIDER_H
