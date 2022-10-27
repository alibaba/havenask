#include "build_service/document/ExtendDocument.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);

namespace build_service {
namespace document {
BS_LOG_SETUP(document, ExtendDocument);

const string ExtendDocument::INDEX_SCHEMA = "index_schema";
const string ExtendDocument::DOC_OPERATION_TYPE = "doc_operation_type";

ExtendDocument::ExtendDocument()
    : _identifier("")
    , _buildInWarningFlags(0)
{
    _indexExtendDoc.reset(new IndexlibExtendDocument);
    _processedDocument.reset(new ProcessedDocument());
}

ExtendDocument::~ExtendDocument()
{
}

}
}
