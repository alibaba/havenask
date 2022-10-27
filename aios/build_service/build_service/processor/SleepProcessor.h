#ifndef ISEARCH_BS_SLEEPPROCESSOR_H
#define ISEARCH_BS_SLEEPPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class SleepProcessor : public DocumentProcessor
{
public:
    SleepProcessor();
    ~SleepProcessor();
private:
    SleepProcessor& operator=(const SleepProcessor &);
public:
    /* override */ bool process(const document::ExtendDocumentPtr &document);
    /* override */ void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    /* override */ DocumentProcessor* clone();
    /* override */ bool init(const DocProcessorInitParam &param);
    /* override */ std::string getDocumentProcessorName() const;
private:
    int64_t _sleepTimePerDoc;
    int64_t _sleepTimeDtor;
private:
    static const std::string SLEEP_PER_DOCUMENT;
    static const std::string SLEEP_DTOR;
public:
    static const std::string PROCESSOR_NAME;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SleepProcessor);

}
}

#endif //ISEARCH_BS_SLEEPPROCESSOR_H
