#ifndef ISEARCH_BS_EXTENDDOCUMENT_H
#define ISEARCH_BS_EXTENDDOCUMENT_H

#include <tr1/memory>
#include <autil/legacy/any.h>
#include <indexlib/document/document.h>
#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include "build_service/document/RawDocument.h"
#include "build_service/document/TokenizeDocument.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/ProcessedDocument.h"

namespace build_service {
namespace document {

enum class ProcessorWarningFlag
{
    PWF_HASH_FIELD_EMPTY = 0x1,
    PWF_ATTR_CONVERT_ERROR = 0x2,
    PWF_PROCESS_FAIL_IN_BATCH = 0x4, 
};
    
class ExtendDocument;
typedef std::tr1::shared_ptr<ExtendDocument> ExtendDocumentPtr;

class ExtendDocument
{
public:
    static const std::string INDEX_SCHEMA;
    static const std::string DOC_OPERATION_TYPE;
    typedef std::map<fieldid_t, std::string> FieldAnalyzerNameMap;
    typedef std::vector<ExtendDocumentPtr> ExtendDocumentVec;
public:
    ExtendDocument();
    ~ExtendDocument();

private:
    ExtendDocument(const ExtendDocument &);
    ExtendDocument& operator = (const ExtendDocument &);
public:
    void setRawDocument(const RawDocumentPtr &doc)
    { _indexExtendDoc->setRawDocument(doc); }
    
    const RawDocumentPtr &getRawDocument() const
    { return _indexExtendDoc->getRawDocument(); }

    const TokenizeDocumentPtr &getTokenizeDocument() const
    { return _indexExtendDoc->getTokenizeDocument(); }

    const ClassifiedDocumentPtr &getClassifiedDocument() const
    { return _indexExtendDoc->getClassifiedDocument(); }

    const ProcessedDocumentPtr &getProcessedDocument() const
    { return _processedDocument; }

    void addSubDocument(ExtendDocumentPtr &document)
    { _subDocuments.push_back(document); }

    ExtendDocumentPtr &getSubDocument(size_t index) {
        if (index >= _subDocuments.size()) {
            static ExtendDocumentPtr emptyExtendDocument;
            return emptyExtendDocument;
        }
        return _subDocuments[index];
    }

    void removeSubDocument(size_t index) {
        if (index < _subDocuments.size()) {
            _subDocuments.erase(_subDocuments.begin() + index);
        }
    }

    size_t getSubDocumentsCount() {
        return _subDocuments.size();
    }

    const ExtendDocumentVec& getAllSubDocuments() const
    { return _subDocuments; }

    void setProperty(const std::string &key, autil::legacy::Any value)
    {_properties[key] = value; }

    autil::legacy::Any getProperty(const std::string &key) const
    {
        std::map<std::string, autil::legacy::Any>::const_iterator it = _properties.find(key);
        if (it != _properties.end())
        {
            return it->second;
        }
        return autil::legacy::Any();
    }

    void setFieldAnalyzerName(fieldid_t fieldId, const std::string& analyzerName) {
        _fieldAnalyzerNameMap[fieldId] = analyzerName;
    }
    std::string getFieldAnalyzerName(fieldid_t fieldId) const {
        FieldAnalyzerNameMap::const_iterator it;
        it = _fieldAnalyzerNameMap.find(fieldId);
        if (it != _fieldAnalyzerNameMap.end()) {
            return it->second;
        } else {
            return "";
        }
    }
    void setFieldAnalyzerNameMap(FieldAnalyzerNameMap fieldAnalyzerNameMap) {
        _fieldAnalyzerNameMap = fieldAnalyzerNameMap;
    }
    const regionid_t& getRegionId() const {
        return _indexExtendDoc->getRegionId();
    }
    void setRegionId(regionid_t regionId) {
        _indexExtendDoc->setRegionId(regionId);
    }

    void setIdentifier(const std::string& identifier) {
        _indexExtendDoc->setIdentifier(identifier);
    }

    const std::string& getIdentifier() const {
        return _indexExtendDoc->getIdentifier();
    }    

    void setWarningFlag(ProcessorWarningFlag warningFlag) {
        _buildInWarningFlags |= static_cast<uint32_t>(warningFlag);
    }

    bool testWarningFlag(ProcessorWarningFlag warningFlag) const {
        uint32_t testFlag = static_cast<uint32_t>(warningFlag) & _buildInWarningFlags;
        return static_cast<uint32_t>(warningFlag) == testFlag;
    }

    const IE_NAMESPACE(document)::IndexlibExtendDocumentPtr& getIndexExtendDoc() const
    { return _indexExtendDoc; }

public:
    //for test
    const FieldAnalyzerNameMap& getFieldAnalyzerNameMap() const {
        return _fieldAnalyzerNameMap;
    }
private:
    IE_NAMESPACE(document)::IndexlibExtendDocumentPtr _indexExtendDoc;
    ProcessedDocumentPtr _processedDocument;
    ExtendDocumentVec _subDocuments;
    std::map<std::string, autil::legacy::Any> _properties;
    FieldAnalyzerNameMap _fieldAnalyzerNameMap;
    std::string _identifier;
    uint32_t _buildInWarningFlags;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //BUILD_EXTENDDOCUMENT_H
