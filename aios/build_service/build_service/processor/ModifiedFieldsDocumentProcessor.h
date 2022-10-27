#ifndef ISEARCH_BS_MODIFIEDFIELDSDOCUMENTPROCESSOR_H
#define ISEARCH_BS_MODIFIEDFIELDSDOCUMENTPROCESSOR_H

#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/ModifiedFieldsModifier.h"
#include <vector>
#include <set>
#include <map>

namespace build_service {
namespace processor {

class ModifiedFieldsDocumentProcessor : public PooledDocumentProcessor
{
public:
    typedef std::vector<ModifiedFieldsModifierPtr> ModifiedFieldsModifierVec;
    typedef std::map<std::string, fieldid_t> FieldIdMap;
    typedef IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet FieldIdSet;
    typedef document::ExtendDocument::ExtendDocumentVec ExtendDocumentVec;

    static const std::string PROCESSOR_NAME;
    static const std::string DERIVATIVE_RELATIONSHIP;
    static const std::string IGNORE_FIELDS;
    static const std::string IGNORE_FIELDS_SEP;
    static const std::string DERIVATIVE_FAMILY_SEP;
    static const std::string DERIVATIVE_PATERNITY_SEP;
    static const std::string DERIVATIVE_BROTHER_SEP;

public:
    ModifiedFieldsDocumentProcessor();
    ~ModifiedFieldsDocumentProcessor();

private:
    ModifiedFieldsDocumentProcessor& operator=(const ModifiedFieldsDocumentProcessor &);

public:
    virtual bool process(const document::ExtendDocumentPtr &document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    virtual DocumentProcessor* clone();
    virtual bool init(const DocProcessorInitParam &param);
    virtual std::string getDocumentProcessorName() const {
        return PROCESSOR_NAME;
    }

private:
    bool parseParamIgnoreFields(const KeyValueMap &kvMap);
    bool parseRelationshipSinglePaternity(const std::string &paternityStr);
    bool parseParamRelationship(const KeyValueMap &kvMap);

    bool isAddDoc(const document::ExtendDocumentPtr &document);
    void initModifiedFieldSet(const document::ExtendDocumentPtr &document,
                              FieldIdSet &unknownFieldIdSet);
    void doInitModifiedFieldSet(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            const document::ExtendDocumentPtr &document,
            FieldIdSet &unknownFieldIdSet);
    bool checkModifiedFieldSet(
            const document::ExtendDocumentPtr &document) const;
    void clearFieldId(const document::ExtendDocumentPtr &document) const;

private:
    ModifiedFieldsModifierVec _ModifiedFieldsModifierVec;
    FieldIdMap _unknownFieldIdMap;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _subSchema;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsDocumentProcessor);

}
}

#endif //ISEARCH_BS_MODIFIEDFIELDSDOCUMENTPROCESSOR_H
