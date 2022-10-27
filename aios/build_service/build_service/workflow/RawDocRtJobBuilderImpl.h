#ifndef ISEARCH_BS_RAWDOCRTJOBBUILDERIMPL_H
#define ISEARCH_BS_RAWDOCRTJOBBUILDERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"

namespace build_service {
namespace workflow {

class DocReaderProducer;

class RawDocRtJobBuilderImpl : public RealtimeBuilderImplBase
{
public:
    RawDocRtJobBuilderImpl(const std::string &configPath,
                        const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
                        const RealtimeBuilderResource& builderResource);
    ~RawDocRtJobBuilderImpl();
private:
    RawDocRtJobBuilderImpl(const RawDocRtJobBuilderImpl &);
    RawDocRtJobBuilderImpl& operator=(const RawDocRtJobBuilderImpl &);
protected:  /*override*/
    bool doStart(const proto::PartitionId &partitionId, KeyValueMap &kvMap);
    bool getLastTimestampInProducer(int64_t &timestamp) override;
    bool getLastReadTimestampInProducer(int64_t &timestamp) override;
    bool producerAndBuilderPrepared() const;
    bool producerSeek(const common::Locator &locator);
    void externalActions() {}
private:
    bool getBuilderAndProducer();
private:
    DocReaderProducer *_producer;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocRtJobBuilderImpl);

}
}

#endif //ISEARCH_BS_RAWDOCRTJOBBUILDERIMPL_H
