/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

class SchemaPatchInfo : public autil::legacy::Jsonizable
{
public:
    class PatchSegmentMeta : public autil::legacy::Jsonizable
    {
    public:
        PatchSegmentMeta(segmentid_t segId = INVALID_SEGMENTID) : mSegId(segId) {}

    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        segmentid_t GetSegmentId() const { return mSegId; }
        const std::vector<std::string>& GetPatchIndexs() const { return mIndexNames; }
        const std::vector<std::string>& GetPatchAttributes() const { return mAttrNames; }

        void AddIndex(const std::string& indexName);
        void AddAttribute(const std::string& attrName);

    private:
        segmentid_t mSegId;
        std::vector<std::string> mIndexNames;
        std::vector<std::string> mAttrNames;
    };
    typedef std::vector<PatchSegmentMeta> PatchSegmentMetas;
    typedef PatchSegmentMetas::const_iterator Iterator;

public:
    SchemaPatchInfo(schemaid_t schemaId = DEFAULT_SCHEMAID) : mSchemaId(schemaId) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    schemaid_t GetSchemaId() const { return mSchemaId; }
    Iterator Begin() const { return mPatchSegmentMetas.begin(); }
    Iterator End() const { return mPatchSegmentMetas.end(); }
    size_t GetPatchSegmentSize() const { return mPatchSegmentMetas.size(); }

    void AddIndex(segmentid_t segId, const std::string& indexName);
    void AddAttribute(segmentid_t segId, const std::string& attrName);

private:
    PatchSegmentMeta& GetPatchSegmentMeta(segmentid_t segId);

private:
    schemaid_t mSchemaId;
    PatchSegmentMetas mPatchSegmentMetas;
};
DEFINE_SHARED_PTR(SchemaPatchInfo);

/////////////////////////////////////////////////////////////////////////////////
class PartitionPatchMeta : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<SchemaPatchInfoPtr> PatchInfoVec;
    class Iterator
    {
    public:
        Iterator(const PatchInfoVec& vec) : mVec(vec), mIter(vec.begin()) {}
        Iterator(const Iterator& iter) : mVec(iter.mVec), mIter(iter.mVec.begin()) {}
        bool HasNext() const { return (mIter != mVec.end()); }
        SchemaPatchInfoPtr Next() { return (*mIter++); }

    private:
        const PatchInfoVec& mVec;
        PatchInfoVec::const_iterator mIter;
    };

public:
    PartitionPatchMeta() {}
    ~PartitionPatchMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AddPatchIndexs(schemaid_t schemaId, segmentid_t segId, const std::vector<std::string>& indexNames);

    void AddPatchAttributes(schemaid_t schemaId, segmentid_t segId, const std::vector<std::string>& attrNames);

    void AddPatchIndex(schemaid_t schemaId, segmentid_t segId, const std::string& indexName);
    void AddPatchAttribute(schemaid_t schemaId, segmentid_t segId, const std::string& attrName);

    schemaid_t GetSchemaIdByIndexName(segmentid_t segId, const std::string& indexName) const;
    schemaid_t GetSchemaIdByAttributeName(segmentid_t segId, const std::string& attrName) const;
    SchemaPatchInfoPtr FindSchemaPatchInfo(schemaid_t schemaId) const;

    void GetSchemaIdsBySegmentId(segmentid_t segId, std::vector<schemaid_t>& schemaIds) const;

public:
    Iterator CreateIterator() const { return Iterator(mSchemaPatchInfos); }
    void Store(const file_system::DirectoryPtr& dir, versionid_t versionId);
    void Load(const file_system::DirectoryPtr& dir, versionid_t versionId);

    std::string ToString() const;
    void FromString(const std::string& content);
    static std::string GetPatchMetaFileName(versionid_t versionId);

private:
    SchemaPatchInfoPtr GetSchemaPatchInfo(schemaid_t schemaId);
    void ResetSchemaIdMap();

private:
    typedef std::pair<segmentid_t, std::string> _Key;
    typedef std::map<_Key, schemaid_t> SchemaIdMap;

private:
    PatchInfoVec mSchemaPatchInfos;
    SchemaIdMap mIndexSchemaIdMap;
    SchemaIdMap mAttrSchemaIdMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatchMeta);
}} // namespace indexlib::index_base
