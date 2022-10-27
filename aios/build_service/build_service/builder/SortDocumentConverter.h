#ifndef ISEARCH_BS_SORTDOCUMENTCONVERTER_H
#define ISEARCH_BS_SORTDOCUMENTCONVERTER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/index_base/index_meta/partition_meta.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <autil/DataBuffer.h>
#include <autil/mem_pool/Pool.h>

namespace build_service {
namespace builder {

struct SortDocument
{
public:
    SortDocument()
    {}
    size_t size() {
        size_t size = sizeof(*this) + _pk.size() + _sortKey.size() + _docStr.size();
        return size;
    }
    IE_NAMESPACE(document)::NormalDocumentPtr deserailize() const {
        autil::DataBuffer dataBuffer(const_cast<char*>(_docStr.data()),
                _docStr.length());
        IE_NAMESPACE(document)::NormalDocumentPtr doc;
        dataBuffer.read(doc);
        return doc;
    }
public:
    DocOperateType _docType;
    autil::ConstString _pk;
    autil::ConstString _sortKey;
    autil::ConstString _docStr;
};

class SortDocumentConverter
{
public:
    SortDocumentConverter();
    virtual ~SortDocumentConverter();
private:
    SortDocumentConverter(const SortDocumentConverter &);
    SortDocumentConverter& operator=(const SortDocumentConverter &);

public:
    size_t size() const {
        return _pool->getUsedBytes();
    }
    virtual void clear() = 0;

    virtual void swap(SortDocumentConverter &other) {
        std::swap(_pool, other._pool);
    }

protected:
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    PoolPtr _pool;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SortDocumentConverter);
}
}

#endif //ISEARCH_BS_SORTDOCUMENTCONVERTER_H
