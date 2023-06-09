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
#ifndef __INDEXLIB_FAKE_PRIMARY_KEY_INDEX_READER_H
#define __INDEXLIB_FAKE_PRIMARY_KEY_INDEX_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyHashConvertor.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace testlib {

template <typename T>
class FakePrimaryKeyIndexReader : public indexlib::index::LegacyPrimaryKeyReader<T>
{
public:
    using SegmentReader = indexlibv2::index::PrimaryKeyDiskIndexer<T>;
    using PKPairTyped = indexlibv2::index::PKPair<T, docid_t>;
    using PKPairVec = std::vector<PKPairTyped>;
    using SegmentReaderPtr = std::shared_ptr<SegmentReader>;

public:
    FakePrimaryKeyIndexReader(index::PrimaryKeyHashType type)
    {
        this->_fieldType = type == index::pk_number_hash ? ft_uint64 : ft_string;
        this->_primaryKeyHashType = type;
        // TODO: Support autil::uint128_t
        assert(typeid(T) == typeid(uint64_t));
        config::PrimaryKeyIndexConfigPtr pkIndexConfig(new config::PrimaryKeyIndexConfig(
            "primaryKey", (typeid(T) == typeid(uint64_t)) ? it_primarykey64 : it_primarykey128));
        pkIndexConfig->SetPrimaryKeyIndexType(pk_sort_array);
        this->_indexConfig = pkIndexConfig;
    }
    ~FakePrimaryKeyIndexReader() {}

public:
    index::AttributeReaderPtr GetLegacyPKAttributeReader() const override
    {
        FakeAttributeReader<T>* attrReader = new FakeAttributeReader<T>();
        attrReader->setAttributeValues(_values);
        return index::AttributeReaderPtr(attrReader);
    }

    docid_t Lookup(const std::string& pkStr, future_lite::Executor* executor) const override
    {
        std::map<std::string, docid_t>::const_iterator it = _map.find(pkStr);
        if (it != _map.end()) {
            docid_t docId = it->second;
            if (docId != INVALID_DOCID && this->_deletionMapReader && this->_deletionMapReader->IsDeleted(docId)) {
                return INVALID_DOCID;
            }
            return docId;
        }

        IE_LOG(INFO, "key: %s not found", pkStr.c_str());
        return INVALID_DOCID;
    }

    docid_t Lookup(const autil::StringView& pkStr) const override { return Lookup(pkStr.to_string(), nullptr); }

    docid_t LookupWithPKHash(const autil::uint128_t& pkHash, future_lite::Executor* executor) const override
    {
        uint64_t pkValue;
        indexlib::index::PrimaryKeyHashConvertor::ToUInt64(pkHash, pkValue);
        return DoLookup(pkValue);
    }

    docid_t DoLookup(uint64_t pk) const
    {
        auto it = _hashKeyMap.find(pk);
        if (it != _hashKeyMap.end()) {
            docid_t docId = it->second;
            if (docId != INVALID_DOCID && this->_deletionMapReader && this->_deletionMapReader->IsDeleted(docId)) {
                return INVALID_DOCID;
            }
            return docId;
        }

        IE_LOG(INFO, "key: %lu not found", pk);
        return INVALID_DOCID;
    }

    void setAttributeValues(const std::string& values) { _values = values; }
    void setPKIndexString(const std::string& mapStr) { convertStringToMap(mapStr); }
    void SetDeletionMapReader(const index::DeletionMapReaderPtr& delReader) { this->_deletionMapReader = delReader; }

    void LoadFakePrimaryKeyIndex(const std::string& fakePkString, const std::string& rootPath)
    {
        // key1:doc_id1,key2:doc_id2;key3:doc_id3,key4:doc_id4
        std::stringstream ss;
        if (!rootPath.empty()) {
            ss << rootPath << "/pk_temp_path_" << pthread_self() << "/";
        } else {
            ss << "./pk_temp_path_" << pthread_self() << "/";
        }
        std::string path = ss.str();
        file_system::FslibWrapper::DeleteDirE(path, file_system::DeleteOption::NoFence(true));
        file_system::FslibWrapper::MkDirE(path);

        file_system::FileSystemOptions options;
        config::LoadConfigList loadConfigList;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        util::MemoryQuotaControllerPtr mqc(new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        util::PartitionMemoryQuotaControllerPtr quotaController(new util::PartitionMemoryQuotaController(mqc));
        options.memoryQuotaController = quotaController;

        this->mFileSystem =
            file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                   /*rootPath=*/path, options, util::MetricProviderPtr(),
                                                   /*isOverride=*/false)
                .GetOrThrow();

        _rootDirectory = file_system::Directory::Get(this->mFileSystem);

        std::vector<std::string> segs = autil::StringTokenizer::tokenize(
            autil::StringView(fakePkString), ";",
            autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        docid_t baseDocId = 0;
        for (size_t x = 0; x < segs.size(); ++x) {
            PKPairVec pairVec;
            std::vector<std::string> kvVector = autil::StringUtil::split(segs[x], ",", true);
            for (size_t i = 0; i < kvVector.size(); i++) {
                std::vector<std::string> kvPair = autil::StringUtil::split(kvVector[i], ":", false);
                assert(kvPair.size() == 2);
                PKPairTyped pkPair;
                // TODO: fix bug, Hash return bool
                pkPair.key = this->Hash(kvPair[0], pkPair.key);
                pkPair.docid = autil::StringUtil::fromString<docid_t>(kvPair[1]);
                pairVec.push_back(pkPair);
            }
            std::sort(pairVec.begin(), pairVec.end());
            pushDataToSegmentList(pairVec, baseDocId, (segmentid_t)x);
            baseDocId += kvVector.size();
        }
    }

private:
    void pushDataToSegmentList(const PKPairVec& pairVec, docid_t baseDocId, segmentid_t segmentId)
    {
        std::stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0";
        file_system::DirectoryPtr segmentDir =
            _rootDirectory->MakeDirectory(ss.str(), file_system::DirectoryOption::Mem());
        file_system::DirectoryPtr primaryKeyDir = segmentDir->MakeDirectory(index::INDEX_DIR_NAME + "/primaryKey");

        file_system::FileWriterPtr fileWriter = primaryKeyDir->CreateFileWriter(index::PRIMARY_KEY_DATA_FILE_NAME);
        assert(fileWriter);
        for (size_t i = 0; i < pairVec.size(); i++) {
            fileWriter->Write((void*)&pairVec[i], sizeof(PKPairTyped)).GetOrThrow();
        }
        fileWriter->Close().GetOrThrow();

        config::PrimaryKeyIndexConfigPtr pkIndexConfig = createIndexConfig();

        indexlibv2::index::IndexerParameter parameter;
        parameter.docCount = pairVec.size();
        SegmentReaderPtr segReader(new SegmentReader(parameter));
        auto status = segReader->Open(pkIndexConfig->MakePrimaryIndexConfigV2(), primaryKeyDir->GetIDirectory());
        assert(status.IsOK());

        this->_segmentReaderList.push_back(
            {std::make_pair(baseDocId, segReader), std::vector<segmentid_t> {segmentId}});
    }

    config::PrimaryKeyIndexConfigPtr createIndexConfig()
    {
        return DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->_indexConfig);
    }

    void convertStringToMap(const std::string& mapStr)
    {
        _values.clear();
        std::vector<std::string> tempVec;
        autil::StringUtil::fromString(mapStr, tempVec, ",");
        for (size_t i = 0; i < tempVec.size(); ++i) {
            std::vector<std::string> valueVec;
            autil::StringUtil::fromString(tempVec[i], valueVec, ":");
            if (valueVec.size() != 2) {
                continue;
            }
            docid_t docid = INVALID_DOCID;
            if (!autil::StringUtil::strToInt32(valueVec[1].c_str(), docid)) {
                docid = INVALID_DOCID;
            }
            _map.insert(make_pair(valueVec[0], docid));
            assert(this->_indexConfig->GetInvertedIndexType() == it_primarykey64);
            uint64_t hashKey;
            index::KeyHasherWrapper::GetHashKey(this->_fieldType, this->_primaryKeyHashType, valueVec[0].data(),
                                                valueVec[0].size(), hashKey);
            _hashKeyMap[hashKey] = docid;
            _values += autil::StringUtil::toString(i) + ",";
        }
    }

private:
    file_system::IFileSystemPtr mFileSystem;
    std::string _values;
    std::map<std::string, docid_t> _map;
    std::map<T, docid_t> _hashKeyMap;
    file_system::DirectoryPtr _rootDirectory;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(testlib, FakePrimaryKeyIndexReader);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_PRIMARY_KEY_INDEX_READER_H
