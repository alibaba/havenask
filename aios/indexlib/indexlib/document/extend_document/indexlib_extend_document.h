#ifndef __INDEXLIB_INDEXLIBEXTEND_DOCUMENT_H
#define __INDEXLIB_INDEXLIBEXTEND_DOCUMENT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/extend_document/tokenize_document.h"
#include "indexlib/document/extend_document/classified_document.h"

DECLARE_REFERENCE_CLASS(document, IndexlibExtendDocument);

IE_NAMESPACE_BEGIN(document);

class IndexlibExtendDocument
{
public:
    typedef std::vector<IndexlibExtendDocumentPtr> IndexlibExtendDocumentVec;
    typedef std::set<fieldid_t> FieldIdSet;
    
public:
    IndexlibExtendDocument()
        : _regionId(DEFAULT_REGIONID)
    {
        _tokenizeDocument.reset(new TokenizeDocument());
        _classifiedDocument.reset(new ClassifiedDocument());
    }
    
    ~IndexlibExtendDocument() {}

public:
    void setRawDocument(const RawDocumentPtr &doc) { _rawDocument = doc; }
    const RawDocumentPtr &getRawDocument() const { return _rawDocument; }

    const TokenizeDocumentPtr &getTokenizeDocument() const
    { return _tokenizeDocument; }

    const ClassifiedDocumentPtr &getClassifiedDocument() const
    { return _classifiedDocument; }

    void addSubDocument(const IndexlibExtendDocumentPtr &document)
    { _subDocuments.push_back(document); }

    const IndexlibExtendDocumentPtr &getSubDocument(size_t index) {
        if (index >= _subDocuments.size()) {
            static IndexlibExtendDocumentPtr emptyIndexlibExtendDocument;
            return emptyIndexlibExtendDocument;
        }
        return _subDocuments[index];
    }

    void removeSubDocument(size_t index) {
        if (index < _subDocuments.size()) {
            _subDocuments.erase(_subDocuments.begin() + index);
        }
    }

    size_t getSubDocumentsCount() { return _subDocuments.size(); }

    const IndexlibExtendDocumentVec& getAllSubDocuments() const
    { return _subDocuments; }

    const regionid_t& getRegionId() const { return _regionId; }
    void setRegionId(regionid_t regionId) { _regionId = regionId; }

    void setIdentifier(const std::string& identifier) { _identifier = identifier; }
    const std::string& getIdentifier() const { return _identifier; }

public:  // modified fields
    // deleted from raw document & move to here
    bool isModifiedFieldSetEmpty() const;
    void clearModifiedFieldSet();

    const FieldIdSet &getModifiedFieldSet() { return _modifiedFields; }    
    void addModifiedField(fieldid_t fid) { _modifiedFields.insert(fid); }
    void removeModifiedField(fieldid_t fid) { _modifiedFields.erase(fid); }
    void setModifiedFieldSet(const FieldIdSet& modifiedFields)
    { _modifiedFields = modifiedFields; }
    bool hasFieldInModifiedFieldSet(fieldid_t fid) const;

    const FieldIdSet &getSubModifiedFieldSet() {return _subModifiedFields; }
    void addSubModifiedField(fieldid_t fid) { _subModifiedFields.insert(fid); }
    void removeSubModifiedField(fieldid_t fid) { _subModifiedFields.erase(fid); }
    bool hasFieldInSubModifiedFieldSet(fieldid_t fid) const;
    void setSubModifiedFieldSet(const FieldIdSet& subModifiedFields)
    { _subModifiedFields = subModifiedFields; }    

private:
    regionid_t _regionId;
    RawDocumentPtr _rawDocument;
    TokenizeDocumentPtr _tokenizeDocument;
    ClassifiedDocumentPtr _classifiedDocument;
    IndexlibExtendDocumentVec _subDocuments;
    FieldIdSet _modifiedFields;
    FieldIdSet _subModifiedFields;
    std::string _identifier;

private:
    friend class IndexlibExtendDocumentTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibExtendDocument);
///////////////////////////////////////////////////////////////////////////////////
inline bool IndexlibExtendDocument::hasFieldInModifiedFieldSet(fieldid_t fid) const
{
    return _modifiedFields.count(fid) > 0;
}

inline bool IndexlibExtendDocument::isModifiedFieldSetEmpty() const
{
    return _modifiedFields.empty() && _subModifiedFields.empty();
}

inline bool IndexlibExtendDocument::hasFieldInSubModifiedFieldSet(fieldid_t fid) const
{
    return _subModifiedFields.count(fid) > 0;
}

inline void IndexlibExtendDocument::clearModifiedFieldSet()
{
    _modifiedFields.clear();
    _subModifiedFields.clear();
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_INDEXLIBEXTEND_DOCUMENT_H

