#ifndef __INDEXLIB_RAW_DOCUMENT_FIELD_EXTRACTOR_H
#define __INDEXLIB_RAW_DOCUMENT_FIELD_EXTRACTOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(document, SearchSummaryDocument);

IE_NAMESPACE_BEGIN(partition);

enum SeekStatus
{
    SS_OK = 0,
    SS_DELETED,
    SS_ERROR
};

class RawDocumentFieldExtractor
{
public:
    class FieldIterator
    {
    public:
        FieldIterator(const std::vector<std::string>& fieldNames,
                      const std::vector<std::string>& fieldValues)
            : names(fieldNames)
            , values(fieldValues)
            , cur(0) {};
        bool HasNext() const { return cur < names.size(); }
        void Next(std::string& name, std::string& value)
        {
            name = names[cur];
            value = values[cur];
            cur++;
        }
    private:
        const std::vector<std::string>& names;
        const std::vector<std::string>& values;
        size_t cur;
    };

    struct FieldInfo
    {
        FieldInfo() : summaryFieldId(INVALID_SUMMARYFIELDID) {}
        index::AttributeReaderPtr attrReader;
        summaryfieldid_t summaryFieldId;  
    };

public:
    RawDocumentFieldExtractor();
    ~RawDocumentFieldExtractor();
public:
    bool Init(const IndexPartitionReaderPtr& indexReader,
              const std::vector<std::string>& fieldNames);

    SeekStatus Seek(docid_t docId);
    FieldIterator CreateIterator();

private:
    bool ValidateFields(const config::IndexPartitionSchemaPtr& schema,
                        const std::vector<std::string>& fieldNames) const;

    bool JudgeReadFromSummary(const config::IndexPartitionSchemaPtr& schema,
                              const std::vector<std::string>& fieldNames) const;

    bool IsFieldInAttribute(const config::AttributeSchemaPtr& attrSchema,
                            const std::string& fieldName) const;

    bool IsFieldInPackAttribute(const config::AttributeSchemaPtr& attrSchema,
                                const std::string& fieldName) const;

    bool IsFieldInSummary(const config::SummarySchemaPtr& summarySchema,
                          const std::string& fieldName) const;

    void GenerateFieldInfos(const config::IndexPartitionSchemaPtr& schema,
                            const IndexPartitionReaderPtr& indexReader,
                            bool preferReadFromSummary,
                            const std::vector<std::string>& fieldNames);
    
    SeekStatus FillFieldValues(docid_t docId, std::vector<std::string>& fieldValues);
    
private:
    friend class RawDocumentFieldExtractorTest;
    static const size_t MAX_POOL_MEMORY_USE = 64 * 1024 * 1024; // 64M

private:
    std::vector<std::string> mFieldNames;
    std::vector<FieldInfo> mFieldInfos;
    std::vector<std::string> mFieldValues;
    document::SearchSummaryDocumentPtr mSummaryDoc;
    index::SummaryReaderPtr mSummaryReader;
    index::DeletionMapReaderPtr mDeletionMapReader;
    autil::mem_pool::Pool mPool;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawDocumentFieldExtractor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_RAW_DOCUMENT_FIELD_EXTRACTOR_H
