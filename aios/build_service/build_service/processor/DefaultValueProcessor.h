#ifndef ISEARCH_BS_DEFAULTVALUEPROCESSOR_H
#define ISEARCH_BS_DEFAULTVALUEPROCESSOR_H

#include "autil/ConstString.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class DefaultValueProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    DefaultValueProcessor();
    DefaultValueProcessor(const DefaultValueProcessor &other);
    ~DefaultValueProcessor();
private:
    DefaultValueProcessor& operator=(const DefaultValueProcessor &);
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
    typedef std::map<autil::ConstString, autil::ConstString> FieldDefaultValueMap; // fieldName : defaultValue in string format
    FieldDefaultValueMap _fieldDefaultValueMap;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DefaultValueProcessor);

}
}

#endif //ISEARCH_BS_DUPFIELDPROCESSOR_H
