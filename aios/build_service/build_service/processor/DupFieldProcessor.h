#ifndef ISEARCH_BS_DUPFIELDPROCESSOR_H
#define ISEARCH_BS_DUPFIELDPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class DupFieldProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    DupFieldProcessor();
    DupFieldProcessor(const DupFieldProcessor &other);
    ~DupFieldProcessor();
private:
    DupFieldProcessor& operator=(const DupFieldProcessor &);
public:
    virtual bool process(const document::ExtendDocumentPtr &document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    virtual DocumentProcessor* clone();
    virtual bool init(const KeyValueMap &kvMap,
                      const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                      config::ResourceReader *resourceReader);

    virtual std::string getDocumentProcessorName() const {return PROCESSOR_NAME;}
private:
    typedef std::map<std::string, std::string> DupFieldMap; // to : from
    DupFieldMap _dupFields;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DupFieldProcessor);

}
}

#endif //ISEARCH_BS_DUPFIELDPROCESSOR_H
