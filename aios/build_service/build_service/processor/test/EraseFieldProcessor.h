#ifndef ISEARCH_BS_ERASEFIELDPROCESSOR_H
#define ISEARCH_BS_ERASEFIELDPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class EraseFieldProcessor : public DocumentProcessor
{
public:
    EraseFieldProcessor();
    ~EraseFieldProcessor();
public:
    /* override */ bool process(const document::ExtendDocumentPtr &document);
    /* override */ bool init(const DocProcessorInitParam &param);
    /* override */ std::string getDocumentProcessorName() const {return "EraseFieldProcessor";}
    /* override */ DocumentProcessor *clone();
private:
    std::vector<std::string> _needEraseFieldNames;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EraseFieldProcessor);

}
}

#endif //ISEARCH_BS_ERASEFIELDPROCESSOR_H
