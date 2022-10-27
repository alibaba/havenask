#ifndef ISEARCH_BS_RAWDOCRTSERVICEBUILDERIMPL_H
#define ISEARCH_BS_RAWDOCRTSERVICEBUILDERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"

namespace build_service {
namespace workflow {

class DocReaderProducer;

class RawDocRtServiceBuilderImpl : public RealtimeBuilderImplBase
{
public:
    RawDocRtServiceBuilderImpl(const std::string &configPath,
                        const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
                        const RealtimeBuilderResource& builderResource);
    virtual ~RawDocRtServiceBuilderImpl();
private:
    RawDocRtServiceBuilderImpl(const RawDocRtServiceBuilderImpl &);
    RawDocRtServiceBuilderImpl& operator=(const RawDocRtServiceBuilderImpl &);
protected:  
    bool doStart(const proto::PartitionId &partitionId, KeyValueMap &kvMap) override;
    bool getLastTimestampInProducer(int64_t &timestamp) override;
    bool getLastReadTimestampInProducer(int64_t &timestamp) override;
    bool producerAndBuilderPrepared() const override;
    bool producerSeek(const common::Locator &locator) override;
    virtual void externalActions() override;
    bool seekProducerToLatest(std::pair<int64_t, int64_t>& forceSeekInfo) override;
    
private:
    bool getBuilderAndProducer();
private:
    DocReaderProducer *_producer;
    bool _startSkipCalibrateDone;    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocRtServiceBuilderImpl);

}
}

#endif //ISEARCH_BS_RAWDOCRTSERVICEBUILDERIMPL_H
