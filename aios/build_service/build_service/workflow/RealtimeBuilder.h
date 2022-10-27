#ifndef ISEARCH_BS_REALTIMEBUILDER_H
#define ISEARCH_BS_REALTIMEBUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include <indexlib/partition/index_partition.h>

BS_DECLARE_REFERENCE_CLASS(builder, Builder);

namespace build_service {
namespace workflow {

class RealtimeBuilderImplBase;

class RealtimeBuilder
{
public:
    RealtimeBuilder(
            const std::string &configPath,
            const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,            
            const RealtimeBuilderResource& builderResource);
    virtual ~RealtimeBuilder();
private:
    RealtimeBuilder(const RealtimeBuilder &);
    RealtimeBuilder& operator=(const RealtimeBuilder &);
public:
    bool start(const proto::PartitionId &partitionId);
    void stop();
    void suspendBuild();
    void resumeBuild();
    bool isRecovered();
    void forceRecover();
    void setTimestampToSkip(int64_t timestamp);
    void getErrorInfo(RealtimeErrorCode &errorCode,
                      std::string &errorMsg,
                      int64_t &errorTime /* time in us */) const;

    builder::Builder* GetBuilder() const;
private:
    RealtimeBuilderImplBase *createRealtimeBuilderImpl(
            const proto::PartitionId &partitionId);
    bool getRealtimeInfo(KeyValueMap &kvMap);
    bool parseRealtimeInfo(const std::string &realtimeInfo, KeyValueMap &kvMap);
private:    
    //virtual for test
    virtual std::string getIndexRoot() const;
    virtual bool downloadConfig(const std::string &indexRoot, KeyValueMap &kvMap);

private:
    RealtimeBuilderImplBase *_impl;
private:
    std::string _configPath;
    IE_NAMESPACE(partition)::IndexPartitionPtr _indexPartition;
    RealtimeBuilderResource _builderResource;
    KeyValueMap _kvMap;
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_REALTIMEBUILDER_H
