#ifndef ISEARCH_BS_REGIONDOCUMENTPROCESSOR_H
#define ISEARCH_BS_REGIONDOCUMENTPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class RegionDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    RegionDocumentProcessor();
    ~RegionDocumentProcessor();
    
public:
    bool init(const DocProcessorInitParam &param) override;
    bool process(const document::ExtendDocumentPtr &document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs) override;
    
public:
    void destroy() override {
        delete this;
    }
    DocumentProcessor* clone() override {
        return new RegionDocumentProcessor(*this);
    }
    std::string getDocumentProcessorName() const override {
        return PROCESSOR_NAME;
    }

private:
    regionid_t _specificRegionId;
    std::string _regionFieldName;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RegionDocumentProcessor);

}
}

#endif //ISEARCH_BS_REGIONDOCUMENTPROCESSOR_H
