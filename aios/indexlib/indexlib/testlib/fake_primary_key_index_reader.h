#ifndef __INDEXLIB_FAKE_PRIMARY_KEY_INDEX_READER_H
#define __INDEXLIB_FAKE_PRIMARY_KEY_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <indexlib/index/normal/primarykey/primary_key_index_reader_typed.h>

IE_NAMESPACE_BEGIN(testlib);

template<typename T>
class FakePrimaryKeyIndexReader : public index::PrimaryKeyIndexReaderTyped<T>
{
public:
    typedef index::PrimaryKeySegmentReaderTyped<T> SegmentReader;
    typedef typename index::PrimaryKeySegmentReaderTyped<T>::PKPair PKPair;
    typedef std::vector<PKPair> PKPairVec;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;

public:
    FakePrimaryKeyIndexReader(PrimaryKeyHashType type) {
        this->mPrimaryKeyHashType = type;
        this->mHashFunc.reset(util::KeyHasherFactory::CreatePrimaryKeyHasher(
                        type == pk_number_hash ? ft_uint64 : ft_string, type));
        config::PrimaryKeyIndexConfigPtr pkIndexConfig(
                new config::PrimaryKeyIndexConfig("primaryKey",
                        (typeid(T) == typeid(uint64_t)) ? it_primarykey64 : it_primarykey128));
        pkIndexConfig->SetPrimaryKeyIndexType(pk_sort_array);
        this->mIndexConfig = pkIndexConfig;
    }
    ~FakePrimaryKeyIndexReader() {}

public:
    index::AttributeReaderPtr GetPKAttributeReader() const override{
        FakeAttributeReader<T> *attrReader = new FakeAttributeReader<T>();
        attrReader->setAttributeValues(_values);
        return index::AttributeReaderPtr(attrReader);
    }

    docid_t Lookup(const std::string &pkStr) const override
    {
        std::map<std::string, docid_t>::const_iterator it = _map.find(pkStr);
        if(it != _map.end()){
            docid_t docId = it->second;
            if (docId != INVALID_DOCID
                && this->mDeletionMapReader && this->mDeletionMapReader->IsDeleted(docId)) {
                return INVALID_DOCID;
            }
            return docId;
        }

        IE_LOG(INFO, "key: %s not found", pkStr.c_str());
        return INVALID_DOCID;
    }

    docid_t Lookup(const autil::ConstString &pkStr) const override
    {
        return Lookup(pkStr.toString());
    }

    docid_t LookupWithNumber(uint64_t pk) const override
    {
        if (mPrimaryKeyHashType == pk_number_hash) {
            return DoLookup(pk);
        }
        return Lookup(autil::StringUtil::toString(pk));
    }

    docid_t LookupWithPKHash(const autil::uint128_t& pkHash) const override
    {
        uint64_t pkValue;
        index::PrimaryKeyHashConvertor::ToUInt64(pkHash, pkValue);
        return DoLookup(pkValue);
    }

    docid_t DoLookup(uint64_t pk) const
    {
        auto it = _hashKeyMap.find(pk);
        if(it != _hashKeyMap.end()){
            docid_t docId = it->second;
            if (docId != INVALID_DOCID
                && this->mDeletionMapReader && this->mDeletionMapReader->IsDeleted(docId)) {
                return INVALID_DOCID;
            }
            return docId;
        }

        IE_LOG(INFO, "key: %lu not found", pk);
        return INVALID_DOCID;
    }

    void setAttributeValues(const std::string &values) {_values = values;}
    void setPKIndexString( const std::string &mapStr) {convertStringToMap(mapStr);}
    void SetDeletionMapReader(const index::DeletionMapReaderPtr &delReader)
    { this->mDeletionMapReader = delReader; }

    void LoadFakePrimaryKeyIndex(const std::string &fakePkString, const std::string &rootPath)
    {
        //key1:doc_id1,key2:doc_id2;key3:doc_id3,key4:doc_id4
        std::stringstream ss;
        if (!rootPath.empty()) {
            ss << rootPath <<"/pk_temp_path_" << pthread_self() << "/";
        } else {
            ss << "./pk_temp_path_" << pthread_self() << "/";
        }
        std::string path = ss.str();
        storage::FileSystemWrapper::DeleteIfExist(path);
        storage::FileSystemWrapper::MkDir(path);

        file_system::FileSystemOptions options;
        config::LoadConfigList loadConfigList;
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = false;
        options.needFlush = true;
        options.useCache = true;
        this->mFileSystem.reset(new file_system::IndexlibFileSystemImpl(path));
        util::MemoryQuotaControllerPtr mqc(
                new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        util::PartitionMemoryQuotaControllerPtr quotaController(
                new util::PartitionMemoryQuotaController(mqc));
        options.memoryQuotaController = quotaController;
        this->mFileSystem->Init(options);

        _rootDirectory = file_system::DirectoryCreator::Get(this->mFileSystem, path, true);

        std::vector<std::string> segs = autil::StringTokenizer::tokenize(
                autil::ConstString(fakePkString),
                ";", autil::StringTokenizer::TOKEN_TRIM
                | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        docid_t baseDocId = 0;
        for (size_t x = 0; x < segs.size(); ++x)
        {
            PKPairVec pairVec;
            std::vector<std::string> kvVector =
                autil::StringUtil::split(segs[x], ",", true);
            for (size_t i = 0; i < kvVector.size(); i++) {
                std::vector<std::string> kvPair = autil::StringUtil::split(kvVector[i], ":", true);
                assert(kvPair.size() == 2);
                PKPair pkPair;
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
    void pushDataToSegmentList(const PKPairVec& pairVec,
                               docid_t baseDocId,
                               segmentid_t segmentId)
    {
        std::stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0";
        file_system::DirectoryPtr segmentDir =
            _rootDirectory->MakeInMemDirectory(ss.str());
        file_system::DirectoryPtr primaryKeyDir = segmentDir->MakeDirectory(
                std::string(INDEX_DIR_NAME) + "/primaryKey");

        file_system::FileWriterPtr fileWriter = primaryKeyDir->CreateFileWriter(
                PRIMARY_KEY_DATA_FILE_NAME);
        assert(fileWriter);
        for (size_t i = 0; i < pairVec.size(); i++) {
            fileWriter->Write((void*)&pairVec[i], sizeof(PKPair));
        }
        fileWriter->Close();

        index_base::SegmentInfo segInfo;
        segInfo.docCount = pairVec.size();
        index_base::SegmentData segData;
        segData.SetDirectory(segmentDir);
        segData.SetSegmentInfo(segInfo);
        segData.SetBaseDocId(baseDocId);
        segData.SetSegmentId(segmentId);
        config::PrimaryKeyIndexConfigPtr pkIndexConfig = createIndexConfig();
        index::PrimaryKeyLoadPlanPtr plan(new index::PrimaryKeyLoadPlan());
        plan->Init(baseDocId, pkIndexConfig);
        plan->AddSegmentData(segData, 0);
        file_system::FileReaderPtr fileReader =
            index::PrimaryKeyLoader<T>::Load(plan, pkIndexConfig);
        SegmentReaderPtr segReader(new SegmentReader);
        segReader->Init(pkIndexConfig, fileReader);

        this->mSegmentList.push_back(std::make_pair(baseDocId, segReader));
    }

    config::PrimaryKeyIndexConfigPtr createIndexConfig()
    {
        return DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->mIndexConfig);
    }

    void convertStringToMap(const std::string &mapStr) {
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
            T hashKey;
            this->mHashFunc->GetHashKey(valueVec[0].data(), valueVec[0].size(), hashKey);
            _hashKeyMap[hashKey] = docid;
            _values += autil::StringUtil::toString(i) + ",";
        }
    }


private:
    PrimaryKeyHashType mPrimaryKeyHashType;
    file_system::IndexlibFileSystemPtr mFileSystem;
    std::string _values;
    std::map<std::string, docid_t> _map;
    std::map<T, docid_t> _hashKeyMap;
    file_system::DirectoryPtr _rootDirectory;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(testlib, FakePrimaryKeyIndexReader);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_PRIMARY_KEY_INDEX_READER_H
