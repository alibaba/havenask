#ifndef __INDEXLIB_SUMMARY_FORMATTER_H
#define __INDEXLIB_SUMMARY_FORMATTER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"

IE_NAMESPACE_BEGIN(document);

class SummaryFormatter
{
public:
    SummaryFormatter(const config::SummarySchemaPtr& summarySchema);
    SummaryFormatter(const SummaryFormatter& other) = delete;
    ~SummaryFormatter() {}

public:
    // caller: build_service::ClassifiedDocument::serializeSummaryDocument
    void SerializeSummaryDoc(const document::SummaryDocumentPtr& document,
                             document::SerializedSummaryDocumentPtr& serDoc);

public:
    // public for test
    size_t GetSerializeLength(const document::SummaryDocumentPtr& document);

    void SerializeSummary(const document::SummaryDocumentPtr& document,
                          char* value, size_t length) const;

    void TEST_DeserializeSummaryDoc(const document::SerializedSummaryDocumentPtr& serDoc,
                                    document::SearchSummaryDocument* document);

    bool TEST_DeserializeSummary(document::SearchSummaryDocument* document,
                                 const char* value, size_t length) const;

private:
    typedef std::vector<SummaryGroupFormatterPtr> GroupFormatterVec;
    typedef std::vector<uint32_t> LengthVec;
    GroupFormatterVec mGroupFormatterVec;
    LengthVec mLengthVec;
    config::SummarySchemaPtr mSummarySchema;

private:
    friend class SummaryFormatterTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SummaryFormatter> SummaryFormatterPtr;

///////////////////////////////////////////////////////////////////////////////

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SUMMARY_FORMATTER_H
