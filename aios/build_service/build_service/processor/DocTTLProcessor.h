#ifndef ISEARCH_BS_DOCTTLPROCESSOR_H
#define ISEARCH_BS_DOCTTLPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class DocTTLProcessor : public DocumentProcessor
{
public:
    DocTTLProcessor();
    ~DocTTLProcessor();
public:
    virtual bool init(const DocProcessorInitParam &param);
    virtual bool process(const document::ExtendDocumentPtr &document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual DocumentProcessor* clone();
    virtual std::string getDocumentProcessorName() const { return "DocTTLProcessor"; }
private:
    uint32_t _ttlTime;
    std::string _ttlFieldName;
public:
    static const std::string PROCESSOR_NAME;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocTTLProcessor);

}
}

#endif //ISEARCH_BS_DOCTTLPROCESSOR_H
