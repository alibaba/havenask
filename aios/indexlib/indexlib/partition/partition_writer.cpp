#include "indexlib/partition/partition_writer.h"
#include "indexlib/document/document.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionWriter);

bool PartitionWriter::NeedRewriteDocument(const document::DocumentPtr& doc)
{
    return doc->GetDocOperateType() == ADD_DOC;
}

IE_NAMESPACE_END(partition);

