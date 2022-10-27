#ifndef __INDEXLIB_ERRORLOGCOLLECTOR_H
#define __INDEXLIB_ERRORLOGCOLLECTOR_H

#include <tr1/memory>
#include <alog/Logger.h>
#include <alog/Configurator.h>
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include <atomic>

#define ERROR_COLLECTOR_LOG(level, format, args...)                     \
    if (heavenask::indexlib::misc::ErrorLogCollector::UsingErrorLogCollect()) \
    {                                                                   \
        do {                                                            \
            thread_local int64_t logCounter = 0;                        \
            logCounter++;                                               \
            heavenask::indexlib::misc::ErrorLogCollector::IncTotalErrorCount(); \
            if (logCounter == (logCounter & -logCounter)) {             \
                ALOG_LOG(heavenask::indexlib::misc::ErrorLogCollector::GetLogger(), \
                         alog::LOG_LEVEL_##level, "%s, error_count[%ld], error_msg: " format, \
                        heavenask::indexlib::misc::ErrorLogCollector::GetIdentifier().c_str(), \
                        logCounter, ##args);                            \
            }                                                           \
        } while (0);                                                    \
    }                                                           

#define DECLARE_ERROR_COLLECTOR_IDENTIFIER(identifier)                  \
    heavenask::indexlib::misc::ErrorLogCollector::SetIdentifier(identifier); \
    char* envStr = getenv("COLLECT_ERROR_LOG");                         \
    if (envStr && std::string(envStr) == "true")                        \
    {                                                                   \
        heavenask::indexlib::misc::ErrorLogCollector::EnableErrorLogCollect(); \
    }

IE_NAMESPACE_BEGIN(misc);

class ErrorLogCollector
{
public:
    ErrorLogCollector();
    ~ErrorLogCollector();
public:
    //generation, role name, errorCount, errorMsg
    static void SetIdentifier(const std::string& identifier)
    {
        mIdentifier = identifier;
    }
    static alog::Logger * GetLogger()
    {
        return mLogger;
    }
    static std::string& GetIdentifier()
    {
        return mIdentifier;
    }
    static bool UsingErrorLogCollect()
    {
        return mUseErrorLogCollector;
    }
    static void EnableErrorLogCollect()
    {
        mUseErrorLogCollector = true;
    }

    static int64_t GetTotalErrorCount()
    {
        return mTotalErrorCount;
    }
    static void IncTotalErrorCount()
    {
        if (mEnableTotalErrorCount)
        {
            mTotalErrorCount += 1;
        }
    }
    static void ResetTotalErrorCount()
    {
        mTotalErrorCount = 0;
    }
    static void EnableTotalErrorCount()
    {
        mEnableTotalErrorCount = true;
    }

private:
    static alog::Logger *mLogger;
    static std::string mIdentifier;
    static bool mUseErrorLogCollector;
    static bool mEnableTotalErrorCount;
    static std::atomic_long mTotalErrorCount;
};


IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_ERRORLOGCOLLECTOR_H
