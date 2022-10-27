#ifndef ISEARCH_BS_DOCTRACEPROCESSOR_H
#define ISEARCH_BS_DOCTRACEPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class DocTraceProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    DocTraceProcessor();
    ~DocTraceProcessor();
    DocTraceProcessor(const DocTraceProcessor &other);

private:
    DocTraceProcessor& operator=(const DocTraceProcessor &);

public:
    bool process(const document::ExtendDocumentPtr &document) override;
    void destroy() override;
    DocumentProcessor* clone();
    bool init(const KeyValueMap &kvMap,
              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
              config::ResourceReader *resourceReader) override;

    std::string getDocumentProcessorName() const override {return PROCESSOR_NAME;}

private:
    bool initMatchRules(const std::string& matchRules);
    bool match(const document::RawDocumentPtr& rawDoc);
private:
    struct MatchItem {
        std::string fieldName;
        std::string matchValue;
    };
    typedef std::vector<MatchItem> MatchRule;
    typedef std::vector<MatchRule> MatchRules;

    int64_t _matchCount;
    int64_t _sampleFreq;
    MatchRules _matchRules;
    std::string _traceField;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocTraceProcessor);

}
}

#endif //ISEARCH_BS_DOCTRACEPROCESSOR_H
