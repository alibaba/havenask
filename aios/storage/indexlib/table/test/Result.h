#pragma once

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/table/test/RawDocument.h"

namespace indexlibv2 { namespace table {

class Result
{
public:
    Result() = default;
    ~Result() = default;

public:
    bool HasError() { return _hasError; }
    void SetError(bool hasError) { _hasError = hasError; }
    void AddDoc(const std::shared_ptr<RawDocument>& doc) { _docs.push_back(doc); }
    size_t GetDocCount() const { return _docs.size(); }
    std::shared_ptr<RawDocument> GetDoc(size_t i) const { return _docs[i]; }
    std::string DebugString()
    {
        std::vector<docid_t> docIds;
        for (const std::shared_ptr<RawDocument>& doc : _docs) {
            docIds.push_back(doc->GetDocId());
        }
        return autil::StringUtil::toString(docIds, ",");
    }

private:
    bool _hasError = false;
    std::vector<std::shared_ptr<RawDocument>> _docs;
};
}} // namespace indexlibv2::table
