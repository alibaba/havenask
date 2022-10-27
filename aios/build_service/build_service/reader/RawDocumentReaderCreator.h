#ifndef ISEARCH_BS_RAWDOCUMENTREADERCREATOR_H
#define ISEARCH_BS_RAWDOCUMENTREADERCREATOR_H

#include <indexlib/document/document_factory.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/reader/ReaderManager.h"
#include "build_service/proto/ErrorCollector.h"

namespace build_service {
namespace util {
    class SwiftClientCreator;
    BS_TYPEDEF_PTR(SwiftClientCreator);
}
}

namespace build_service {
namespace reader {

class RawDocumentReaderCreator : public proto::ErrorCollector
{
public:
    RawDocumentReaderCreator(const util::SwiftClientCreatorPtr& swiftClientCreator);
    virtual ~RawDocumentReaderCreator();
private:
    RawDocumentReaderCreator(const RawDocumentReaderCreator &);
    RawDocumentReaderCreator& operator=(const RawDocumentReaderCreator &);
public:
    // virtual for mock
    virtual RawDocumentReader *create(
            const config::ResourceReaderPtr &resourceReader,
            const KeyValueMap &kvMap,
            const proto::PartitionId &partitionId,
            IE_NAMESPACE(misc)::MetricProviderPtr metricsProvider,
            const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema = IE_NAMESPACE(config)::IndexPartitionSchemaPtr());
private:
    RawDocumentReader *createSingleSourceReader(
            const config::ResourceReaderPtr &resourceReader,
            const KeyValueMap &kvMap,
            const proto::PartitionId &partitionId,
            IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
            const IE_NAMESPACE(util)::CounterMapPtr &counterMap,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema);
private:
    ReaderManagerPtr _readerManagerPtr;
    util::SwiftClientCreatorPtr _swiftClientCreator;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocumentReaderCreator);

}
}

#endif //ISEARCH_BS_RAWDOCUMENTREADERCREATOR_H
