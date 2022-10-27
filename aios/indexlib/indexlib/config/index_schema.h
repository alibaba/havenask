#ifndef __INDEXLIB_INDEX_SCHEMA_H
#define __INDEXLIB_INDEX_SCHEMA_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/modify_item.h"

DECLARE_REFERENCE_CLASS(config, IndexSchemaImpl);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

class IndexSchema : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<IndexConfigPtr> IndexConfigVector;
    typedef std::map<std::string, indexid_t> NameMap;
    typedef std::vector<std::vector<indexid_t> > FlagVector;

public:
    typedef IndexConfigVector::const_iterator Iterator;

public:
    IndexSchema();
    ~IndexSchema() {}

public:
    void AddIndexConfig(const IndexConfigPtr& indexInfo);

    // return NULL if index is is_deleted or not exist
    // return object if index is is_normal or is_disable (maybe not ready)
    IndexConfigPtr GetIndexConfig(const std::string& indexName) const;
    IndexConfigPtr GetIndexConfig(indexid_t id) const;
    bool IsDeleted(indexid_t id) const;

    // return not include indexid which is deleted, but will include disabled index
    const std::vector<indexid_t>& GetIndexIdList(fieldid_t fieldId) const;

    // index count include deleted & disabled index
    size_t GetIndexCount() const; 

    // Begin & End include deleted & disabled index
    Iterator Begin() const;
    Iterator End() const;

    // iterator will access to target config object filtered by type
    IndexConfigIteratorPtr CreateIterator(bool needVirtual = false,
                                          ConfigIteratorType type = CIT_NORMAL) const;

    // not include deleted index, include disabled index
    bool IsInIndex(fieldid_t fieldId) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const IndexSchema& other) const;
    void AssertCompatible(const IndexSchema& other) const;

    // API for primary key start
    bool HasPrimaryKeyIndex() const;
    bool HasPrimaryKeyAttribute() const;
    IndexType GetPrimaryKeyIndexType() const;
    std::string GetPrimaryKeyIndexFieldName() const;
    fieldid_t GetPrimaryKeyIndexFieldId() const;
    const SingleFieldIndexConfigPtr& GetPrimaryKeyIndexConfig() const;
    // API for primary key end
    void LoadTruncateTermInfo(const std::string& metaDir);
    void LoadTruncateTermInfo(const file_system::DirectoryPtr& metaDir);
    void Check();

    void DeleteIndex(const std::string& indexName);
    void DisableIndex(const std::string& indexName);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();

    void CollectBaseVersionIndexInfo(ModifyItemVector& indexs) const;
    
public:
    // for test
    void SetPrimaryKeyIndexConfig(const SingleFieldIndexConfigPtr& config);
    
private:
    IndexSchemaImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<IndexSchema> IndexSchemaPtr;
//////////////////////////////////////////////////////////////////////////
IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_SCHEMA_H
