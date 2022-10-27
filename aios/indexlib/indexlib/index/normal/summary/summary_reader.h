#ifndef __INDEXLIB_SUMMARY_READER_H
#define __INDEXLIB_SUMMARY_READER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <tr1/memory>
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/misc/exception.h"

DECLARE_REFERENCE_CLASS(document, SearchSummaryDocument);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, SummarySchema);

IE_NAMESPACE_BEGIN(index);

#define DECLARE_SUMMARY_READER_IDENTIFIER(id)                           \
    static std::string Identifier() { return std::string("summary.reader." #id);} \
    std::string GetIdentifier() const override { return Identifier();}    

class SummaryReader
{
public:
    SummaryReader(const config::SummarySchemaPtr& summarySchema);
    virtual ~SummaryReader();
    
public:
    virtual bool Open(const index_base::PartitionDataPtr& partitionData,
                      const PrimaryKeyIndexReaderPtr& pkIndexReader) 
    { assert(false); return false; }

    virtual bool GetDocument(docid_t docId,
                             document::SearchSummaryDocument *summaryDoc) const = 0;
    virtual bool GetDocument(docid_t docId,
                             document::SearchSummaryDocument *summaryDoc,
                             const SummaryGroupIdVec& groupVec) const
    { assert(false); return false; }

    virtual std::string GetIdentifier() const = 0;

    virtual void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) = 0;
    virtual void AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader)
    { assert(false); }

    bool GetDocumentByPkStr(const std::string &pkStr,
                            document::SearchSummaryDocument *summaryDoc) const;
    bool GetDocumentByPkStr(const std::string &pkStr,
                            document::SearchSummaryDocument *summaryDoc,
                            const SummaryGroupIdVec& groupVec) const;
    template <typename Key>
    bool GetDocumentByPkHash(const Key &key, document::SearchSummaryDocument *summaryDoc);

private:
    void AssertPkIndexExist() const;

protected:
    config::SummarySchemaPtr mSummarySchema;
    PrimaryKeyIndexReaderPtr mPKIndexReader;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SummaryReader> SummaryReaderPtr;

////////////////////////////////////////////////////////////////

inline void SummaryReader::AssertPkIndexExist() const
{
    if (unlikely(!mPKIndexReader))
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "primary key index may not be configured");
    }
}

inline bool SummaryReader::GetDocumentByPkStr(const std::string &pkStr,
                                       document::SearchSummaryDocument *summaryDoc) const
{
    AssertPkIndexExist();
    docid_t docId = mPKIndexReader->Lookup(pkStr);
    return GetDocument(docId, summaryDoc);
}

inline bool SummaryReader::GetDocumentByPkStr(const std::string &pkStr,
                                       document::SearchSummaryDocument *summaryDoc,
                                       const SummaryGroupIdVec& groupVec) const
{
    AssertPkIndexExist();
    docid_t docId = mPKIndexReader->Lookup(pkStr);
    return GetDocument(docId, summaryDoc, groupVec);
}


template <typename Key>
inline bool SummaryReader::GetDocumentByPkHash(const Key &key, document::SearchSummaryDocument *summaryDoc)
{
    AssertPkIndexExist();

    std::tr1::shared_ptr<PrimaryKeyIndexReaderTyped<Key> > pkReader = 
        std::tr1::static_pointer_cast<PrimaryKeyIndexReaderTyped<Key> >(mPKIndexReader);
    if (pkReader.get() == NULL)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "hash key type[%s], indexName[%s]",
                             typeid(key).name(), mPKIndexReader->GetIndexName().c_str());
    }

    docid_t docId = pkReader->Lookup(key);

    if (docId == INVALID_DOCID)
    {
        return false;
    }
    return GetDocument(docId, summaryDoc);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_READER_H
