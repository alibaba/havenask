#ifndef ISEARCH_BS_DOCUMENTPROCESSOR_H
#define ISEARCH_BS_DOCUMENTPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/plugin/PooledObject.h"
#include <indexlib/misc/common.h>
#include <indexlib/config/index_partition_schema.h>
#include "build_service/document/ExtendDocument.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service {
namespace config {
class ResourceReader;
}
}

namespace build_service {
namespace analyzer {
class AnalyzerFactory;
BS_TYPEDEF_PTR(AnalyzerFactory);
}
}

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace processor {

struct DocProcessorInitParam {
    DocProcessorInitParam()
        : resourceReader(NULL)
        , processForSubDoc(false)
    {}
    KeyValueMap parameters;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schemaPtr;
    config::ResourceReader *resourceReader;
    analyzer::AnalyzerFactoryPtr analyzerFactoryPtr;
    std::vector<std::string> clusterNames;
    IE_NAMESPACE(misc)::MetricProviderPtr metricProvider;
    IE_NAMESPACE(util)::CounterMapPtr counterMap;
    bool processForSubDoc;
};

class DocumentProcessor
{
public:
    DocumentProcessor(int docOperateFlag = ADD_DOC)
        : _docOperateFlag(docOperateFlag)
    {}
    virtual ~DocumentProcessor() {}
public:
    // only return false when process failed
    virtual bool process(const document::ExtendDocumentPtr &document) = 0;

    // user should use self-implement batchProcess for performance 
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs)
    {
        for (size_t i = 0; i < docs.size(); i++) {
            bool ret = false;
            if ((i + 1) == docs.size()) {
                ret = process(docs[i]);
            } else {
                DocumentProcessor* cloneDocProcessor = allocate();
                if (cloneDocProcessor) {
                    ret = cloneDocProcessor->process(docs[i]);
                    cloneDocProcessor->deallocate();
                }
            }
            if (!ret) {
                docs[i]->setWarningFlag(
                        document::ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
            }
        }
    }

    virtual void destroy() {
        delete this;
    }
    virtual DocumentProcessor* clone() = 0;
    virtual bool init(const DocProcessorInitParam &param) {
        return init(param.parameters, param.schemaPtr, param.resourceReader);
    }
    virtual std::string getDocumentProcessorName() const {return "DocumentProcessor";}
public:
    bool needProcess(DocOperateType docOperateType) const {
        return (_docOperateFlag & docOperateType) != 0;
    }
public:
    // compatiblity for old implement
    virtual bool init(const KeyValueMap &kvMap,
                      const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                      config::ResourceReader *resourceReader)
    {
        return true;
    }
public:
    // for ha3
    virtual DocumentProcessor* allocate() {
        return clone();
    }
    virtual void deallocate() {
        destroy();
    }
protected:
    int _docOperateFlag;
};

typedef std::tr1::shared_ptr<DocumentProcessor> DocumentProcessorPtr;
typedef plugin::PooledObject<DocumentProcessor> PooledDocumentProcessor;

}
}

#endif //ISEARCH_BS_DOCUMENTPROCESSOR_H
