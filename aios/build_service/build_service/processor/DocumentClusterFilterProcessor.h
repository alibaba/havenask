#ifndef ISEARCH_BS_DOCUMENTCLUSTERFILTERPROCESSOR_H
#define ISEARCH_BS_DOCUMENTCLUSTERFILTERPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include <functional>

namespace build_service {
namespace processor {

class DocumentClusterFilterProcessor: public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string FILTER_RULE;
public:
    DocumentClusterFilterProcessor();
    ~DocumentClusterFilterProcessor();

public:
    bool process(const document::ExtendDocumentPtr &document) override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam &param) override;
    std::string getDocumentProcessorName() const override {return PROCESSOR_NAME; }
    void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs) override;
    
private:
    bool parseRule(const std::string& ruleStr);
private:
    int64_t _value;
    std::string _fieldName;
    std::function<bool(int64_t,int64_t)> _op;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentClusterFilterProcessor);

}
}

#endif //ISEARCH_BS_DOCUMENTCLUSTERFILTERPROCESSOR_H
