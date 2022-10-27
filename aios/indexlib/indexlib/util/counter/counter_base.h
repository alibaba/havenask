#ifndef __INDEXLIB_COUNTER_BASE_H
#define __INDEXLIB_COUNTER_BASE_H

#include <tr1/memory>
#include <autil/legacy/any.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class CounterBase
{
public:
    enum CounterType : uint32_t
    {
        CT_DIRECTORY = 0,
        CT_ACCUMULATIVE,
        CT_STATE,
        CT_UNKNOWN,
    };

    enum FromJsonType : uint32_t
    {
        FJT_OVERWRITE = 0,
        FJT_MERGE,
        FJT_UNKNOWN,
    };
    
    static const std::string TYPE_META;
public:
    CounterBase(const std::string& path, CounterType type);
    virtual ~CounterBase();
public:
    virtual autil::legacy::Any ToJson() const = 0;
    virtual void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) = 0;
    const std::string& GetPath() const { return mPath; }
    CounterType GetType() const { return mType; }    
protected:
    static std::string CounterTypeToStr(CounterType type);
    static CounterType StrToCounterType(const std::string& str);
protected:
    const std::string mPath;
    const CounterType mType;
};

DEFINE_SHARED_PTR(CounterBase);

inline std::string CounterBase::CounterTypeToStr(CounterType type)
{
    switch (type)
    {
    case CT_DIRECTORY: return "DIR";
    case CT_ACCUMULATIVE: return "ACC";
    case CT_STATE: return "STATE";
    default:
        return "UNKNOWN";
    }
}

inline CounterBase::CounterType CounterBase::StrToCounterType(const std::string& str)
{
    if (str == "DIR")
    {
        return CT_DIRECTORY;
    }
    else if (str == "ACC")
    {
        return CT_ACCUMULATIVE;
    }
    else if (str == "STATE")
    {
        return CT_STATE;
    }
    return CT_UNKNOWN;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_COUNTER_BASE_H
