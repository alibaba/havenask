#ifndef __INDEXLIB_LOAD_CONFIG_BASE_H
#define __INDEXLIB_LOAD_CONFIG_BASE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/file_system/load_config/warmup_strategy.h"
#include "indexlib/misc/regular_expression.h"

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigBase : public autil::legacy::Jsonizable
{
public:
    enum class LoadMode
    {
        REMOTE_ONLY,
        LOCAL_ONLY,
    };

public:
    LoadConfigBase();
    ~LoadConfigBase();

public:
    typedef std::vector<std::string> FilePatternStringVector;

public:
    bool Match(const std::string& filePath) const;
    
    const FilePatternStringVector& GetFilePatternString() const
    { return mFilePatterns; }

    void SetFilePatternString(const FilePatternStringVector& filePatterns);

    const misc::RegularExpressionVector& GetFilePatternRegex() const 
    { return mRegexVector; }

    const std::string& GetLoadStrategyName() const
    { return mLoadStrategy->GetLoadStrategyName(); }

    const LoadStrategyPtr& GetLoadStrategy() const 
    { return mLoadStrategy; }
    void SetLoadStrategyPtr(const LoadStrategyPtr& loadStrategy) 
    { mLoadStrategy = loadStrategy; } 

    const WarmupStrategy& GetWarmupStrategy() const
    { return mWarmupStrategy; }
    void  SetWarmupStrategy(const WarmupStrategy& warmupStrategy)
    { mWarmupStrategy = warmupStrategy; }

    void Check() const;

    bool EqualWith(const LoadConfigBase& loadConfig) const;

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);



    void SetName(const std::string name) { mName = name; }

    const std::string& GetName() const { return mName; }

    void SetRemote(bool remote) { mRemote = remote; }
    const bool IsRemote() const { return mRemote; }
    void SetDeploy(bool deploy) { mDeploy = deploy; }
    const bool NeedDeploy() const { return mDeploy; }

    void SetLoadMode(LoadMode mode)
    {
        assert(mode == LoadMode::LOCAL_ONLY || mode == LoadMode::REMOTE_ONLY);
        if (mode == LoadMode::LOCAL_ONLY)
        {
            mRemote = false;
            mDeploy = true;
        }
        else if (mode == LoadMode::REMOTE_ONLY)
        {
            mRemote = true;
            mDeploy = false;
        }
    }

protected:
    static void CreateRegularExpressionVector(
            const FilePatternStringVector& patternVector,
            misc::RegularExpressionVector& regexVector);

    static const std::string& BuiltInPatternToRegexPattern(const std::string& builtInPattern);

protected:
    LoadStrategyPtr mLoadStrategy;
    FilePatternStringVector mFilePatterns;
    // abstract warmup strategy to a class when it's geting complex
    WarmupStrategy mWarmupStrategy;
    misc::RegularExpressionVector mRegexVector;
    std::string mName;
    bool mRemote; // read from remote
    bool mDeploy; // need deploy to local
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LoadConfigBase);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOAD_CONFIG_BASE_H
