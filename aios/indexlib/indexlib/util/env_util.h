#ifndef __INDEXLIB_ENV_UTIL_H
#define __INDEXLIB_ENV_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/StringUtil.h>
#include <stdlib.h>

IE_NAMESPACE_BEGIN(util);


class EnvUtil
{
public:
    EnvUtil() = delete;

public:
    template<typename T>
    static T GetEnv(const std::string& key, const T &defaulValue)
    {
        const char *str = getenv(key.c_str());
        if (!str)
        {
            return defaulValue;
        }
        T ret = T();
        auto success = autil::StringUtil::fromString(str, ret);
        if (success)
        {
            return ret;
        }
        else
        {
            return defaulValue;
        }
    }

    static int SetEnv(const std::string& env, const std::string& value)
    {
        return setenv(env.c_str(), value.c_str(), 1);
    }

    static int UnsetEnv(const std::string& env)
    {
        return unsetenv(env.c_str());
    }

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(EnvUtil);

class EnvGuard
{
public:
    EnvGuard(const std::string& key, const std::string& newValue)
        : mKey(key)
    {
        mOldValue = getenv(key.c_str());
        EnvUtil::SetEnv(key, newValue);
    }
    ~EnvGuard()
    {
        if (!mOldValue)
        {
            EnvUtil::UnsetEnv(mKey);
        }
        else
        {
            EnvUtil::SetEnv(mKey, std::string(mOldValue));
        }
    }
private:
    std::string mKey;
    char* mOldValue;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ENV_UTIL_H
