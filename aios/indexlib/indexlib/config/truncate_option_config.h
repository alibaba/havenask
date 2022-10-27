#ifndef __INDEXLIB_TRUNCATE_OPTION_CONFIG_H
#define __INDEXLIB_TRUNCATE_OPTION_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/config/truncate_index_config.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(config);

typedef std::vector<std::string> StringVec;
typedef std::map<std::string, std::string> ProfileToStrategyNameMap;
typedef std::map<std::string, TruncateIndexConfig> TruncateIndexConfigMap;
typedef std::vector<TruncateStrategyPtr> TruncateStrategyVec;
typedef std::vector<TruncateProfilePtr> TruncateProfileVec;

// todo: remove inherited from Jsonizable 
class TruncateOptionConfig : public autil::legacy::Jsonizable
{
public:
    // for TruncateConfigMaker
    TruncateOptionConfig() {}
    
    TruncateOptionConfig(const std::vector<TruncateStrategy>& truncateStrategyVec);
    ~TruncateOptionConfig();
public:
    // TODO: remove later
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { }
    
    void Check() const;

    bool operator==(const TruncateOptionConfig& other) const;

    bool IsTruncateIndex(const std::string indexName) const;

    const TruncateIndexConfig& GetTruncateIndexConfig(
            const std::string& indexName) const;

    const TruncateProfileVec& GetTruncateProfiles() const 
    { 
        return mTruncateProfileVec; 
    }

    const TruncateProfilePtr& GetTruncateProfile(
            const std::string& profileName) const;

    const TruncateStrategyVec& GetTruncateStrategys() const
    {
        return mTruncateStrategyVec;
    }

    const TruncateStrategyPtr& GetTruncateStrategy(
            const std::string& strategyName) const;

    const TruncateIndexConfigMap& GetTruncateIndexConfigs() const
    {
        return mTruncateIndexConfig;
    }

    void Init(const IndexPartitionSchemaPtr& schema);

    void UpdateTruncateIndexConfig(int64_t beginTime, int64_t baseTime);

public:
    // for test
    void AddTruncateProfile(const TruncateProfilePtr& truncateProfile);
    void AddTruncateStrategy(const TruncateStrategyPtr& truncateStategy);
    const std::vector<TruncateStrategy>& GetOriTruncateStrategy() const
    {
	return mOriTruncateStrategyVec;
    }

private:
    void CompleteTruncateStrategy(TruncateProfilePtr &profilePtr,
                                  const TruncateStrategy &strategy);

    void CreateTruncateIndexConfigs();

    void CompleteTruncateConfig(ProfileToStrategyNameMap &profileToStrategyMap,
				const IndexPartitionSchemaPtr& schema);

    void InitProfileToStrategyNameMap(ProfileToStrategyNameMap &profileToStrategyMap);

private:
    std::vector<TruncateStrategy> mOriTruncateStrategyVec;
    TruncateProfileVec mTruncateProfileVec;
    TruncateStrategyVec mTruncateStrategyVec;
    TruncateIndexConfigMap mTruncateIndexConfig;
    TruncateStrategyPtr mDefaultTruncateStrategyPtr;
    TruncateProfilePtr mDefaultTruncateProfilePtr;

private:
    IE_LOG_DECLARE();
    friend class TruncateOptionConfigTest;
};

DEFINE_SHARED_PTR(TruncateOptionConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_OPTION_CONFIG_H
