#ifndef __INDEXLIB_INDEX_SCHEMA_IMPL_H
#define __INDEXLIB_INDEX_SCHEMA_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <tr1/memory>
#include "indexlib/config/index_config.h"
#include "indexlib/config/modify_item.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

class IndexSchemaImpl : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<IndexConfigPtr> IndexConfigVector;
    typedef std::map<std::string, indexid_t> NameMap;
    typedef std::vector<std::vector<indexid_t> > FlagVector;

public:
    typedef IndexConfigVector::const_iterator Iterator;

public:
    IndexSchemaImpl()
        : mPrimaryKeyIndexId(INVALID_FIELDID)
        , mBaseIndexCount(-1)
        , mOnlyAddVirtual(false)
    {}

    ~IndexSchemaImpl() {}

public:
    void AddIndexConfig(const IndexConfigPtr& indexInfo); 
    IndexConfigPtr GetIndexConfig(const std::string& indexName) const;
    IndexConfigPtr GetIndexConfig(indexid_t id) const;
    bool IsDeleted(indexid_t id) const;    

    const std::vector<indexid_t>& GetIndexIdList(fieldid_t fieldId) const;
    size_t GetIndexCount() const 
    { return mIndexConfigs.size() + mVirtualIndexConfigs.size(); }

    Iterator Begin() const {return mIndexConfigs.begin();}
    Iterator End() const {return mIndexConfigs.end();}
    IndexConfigIteratorPtr CreateIterator(bool needVirtual, ConfigIteratorType type) const;

    bool IsInIndex(fieldid_t fieldId) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const IndexSchemaImpl& other) const;
    void AssertCompatible(const IndexSchemaImpl& other) const;

    // API for primary key start
    bool HasPrimaryKeyIndex() const {return mPrimaryKeyIndexConfig != NULL;}
    bool HasPrimaryKeyAttribute() const;
    IndexType GetPrimaryKeyIndexType() const
    { return mPrimaryKeyIndexConfig->GetIndexType(); }
    std::string GetPrimaryKeyIndexFieldName() const;
    fieldid_t GetPrimaryKeyIndexFieldId() const
    { return mPrimaryKeyIndexId; }
    const SingleFieldIndexConfigPtr& GetPrimaryKeyIndexConfig() const
    { return mPrimaryKeyIndexConfig; }
    // API for primary key end

    void LoadTruncateTermInfo(const std::string& metaDir);
    void LoadTruncateTermInfo(const file_system::DirectoryPtr& metaDir);
    void Check();

    void SetPrimaryKeyIndexConfig(const SingleFieldIndexConfigPtr& config)
    { mPrimaryKeyIndexConfig = config; }

    void DeleteIndex(const std::string& indexName);
    void DisableIndex(const std::string& indexName);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();

    void CollectBaseVersionIndexInfo(ModifyItemVector& indexs);
    
private:
    void AddIndexConfig(const IndexConfigPtr& indexConfig, 
                        NameMap& nameMap,
                        IndexConfigVector& intoxicants);

    void SetExistFlag(const IndexConfigPtr& indexConfig);
    void SetExistFlag(fieldid_t fieldId, indexid_t indexId);

    void SetNonExistFlag(const IndexConfigPtr& indexConfig);
    void SetNonExistFlag(fieldid_t fieldId, indexid_t indexId);

    void CheckTireIndex();
    void CheckIndexNameNotSummary(const IndexConfigPtr& indexConfig) const;
    void ResetVirtualIndexId();

private:
    IndexConfigVector mIndexConfigs;
    IndexConfigVector mVirtualIndexConfigs;
    NameMap mIndexName2IdMap;
    NameMap mVirtualIndexname2IdMap;

    //for IsInIndex
    FlagVector mFlagVector;

    //for primarykey index
    SingleFieldIndexConfigPtr mPrimaryKeyIndexConfig;
    fieldid_t mPrimaryKeyIndexId;
    int32_t mBaseIndexCount;
    bool mOnlyAddVirtual;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<IndexSchemaImpl> IndexSchemaImplPtr;
//////////////////////////////////////////////////////////////////////////
IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_SCHEMA_IMPL_H
