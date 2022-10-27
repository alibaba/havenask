#ifndef ISEARCH_BS_BS_PROCESSEDDOCUMENT_H
#define ISEARCH_BS_BS_PROCESSEDDOCUMENT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/common/Locator.h"
#include <indexlib/document/document.h>

namespace build_service {
namespace document {

class ProcessedDocument {
public:
    struct DocClusterMeta {
        DocClusterMeta() {
            hashId = 0;
            buildType = 0;
        }
        ~DocClusterMeta() {
        }
        std::string clusterName;
        hashid_t hashId;
        // '1' bit will be filter, '0' bit will be read 
        uint8_t buildType;
    };
    static const uint8_t SWIFT_FILTER_BIT_REALTIME = 1;
    static const uint8_t SWIFT_FILTER_BIT_NONE = 0;
    typedef std::vector<DocClusterMeta> DocClusterMetaVec;
public:
    ProcessedDocument()
        : _needSkip(false)
    {}
    ~ProcessedDocument() {}
public:
    void setDocument(const IE_NAMESPACE(document)::DocumentPtr &document);
    const IE_NAMESPACE(document)::DocumentPtr &getDocument() const;

    void setLocator(const common::Locator &locator);
    const common::Locator &getLocator() const;

    void setDocClusterMetaVec(const DocClusterMetaVec &vec);
    void addDocClusterMeta(const DocClusterMeta &meta);
    const DocClusterMetaVec &getDocClusterMetaVec() const;
    bool needSkip() const;
    void setNeedSkip(bool needSkip);
private:
    IE_NAMESPACE(document)::DocumentPtr _document;
    common::Locator _locator;
    DocClusterMetaVec _docClusterMeta;
    bool _needSkip;
};

BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);
/////////////////////////////////////////////////////
inline void ProcessedDocument::setDocument(const IE_NAMESPACE(document)::DocumentPtr &document) {
    _document = document;
}

inline const IE_NAMESPACE(document)::DocumentPtr &ProcessedDocument::getDocument() const {
    return _document;
}

inline void ProcessedDocument::setLocator(const common::Locator &locator) {
    _locator = locator;
}

inline const common::Locator &ProcessedDocument::getLocator() const {
    return _locator;
}

inline void ProcessedDocument::setDocClusterMetaVec(
        const ProcessedDocument::DocClusterMetaVec &vec)
{
    _docClusterMeta = vec;
}

inline void ProcessedDocument::addDocClusterMeta(const DocClusterMeta &meta) {
    _docClusterMeta.push_back(meta);
}

inline const ProcessedDocument::DocClusterMetaVec &ProcessedDocument::getDocClusterMetaVec() const {
    return _docClusterMeta;
}

inline bool ProcessedDocument::needSkip() const {
    return _needSkip;
}

inline void ProcessedDocument::setNeedSkip(bool needSkip) {
    _needSkip = needSkip;
}

}
}

#endif //ISEARCH_BS_BS_PROCESSEDDOCUMENT_H
