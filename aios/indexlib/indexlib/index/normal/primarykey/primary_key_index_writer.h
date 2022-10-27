#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_WRITER_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/document/document.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyIndexWriter : public IndexWriter
{
public:
    PrimaryKeyIndexWriter() {}
    ~PrimaryKeyIndexWriter() {}

public:
    void AddField(const document::Field* field) override {}
    virtual bool CheckPrimaryKeyStr(const std::string& str) const = 0;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PrimaryKeyIndexWriter> PrimaryKeyIndexWriterPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_INDEX_WRITER_H
