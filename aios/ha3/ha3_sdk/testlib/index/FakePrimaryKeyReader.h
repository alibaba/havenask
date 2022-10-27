#ifndef ISEARCH_FAKEPRIMARYKEYREADER_H
#define ISEARCH_FAKEPRIMARYKEYREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeAttributeReader.h>
#include <ha3_sdk/testlib/index/IndexDirectoryCreator.h>
#include <indexlib/index/normal/primarykey/primary_key_index_reader_typed.h>

IE_NAMESPACE_BEGIN(index);

template<typename T>
class FakePrimaryKeyReader : public PrimaryKeyIndexReaderTyped<T>
{
public:
    typedef PrimaryKeySegmentReaderTyped<T> SegmentReader;
    typedef typename PrimaryKeySegmentReaderTyped<T>::PKPair PKPair;
    typedef std::vector<PKPair> PKPairVec;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;

public:
    FakePrimaryKeyReader() {
        this->mHashFunc.reset(util::KeyHasherFactory::CreatePrimaryKeyHasher(ft_string, pk_default_hash));

        config::PrimaryKeyIndexConfigPtr pkIndexConfig(
                new config::PrimaryKeyIndexConfig("primaryKey", 
                        (typeid(T) == typeid(uint64_t)) ? it_primarykey64 : it_primarykey128));
        pkIndexConfig->SetPrimaryKeyIndexType(pk_sort_array);
        this->mIndexConfig = pkIndexConfig;
    }

    ~FakePrimaryKeyReader() {}

public:
    AttributeReaderPtr GetPKAttributeReader() const {
        FakeAttributeReader<T> *attrReader = new FakeAttributeReader<T>();
        attrReader->setAttributeValues(_values);
        return AttributeReaderPtr(attrReader);
    }

    PostingIterator *Lookup(const common::Term& term,
                            uint32_t statePoolSize = 1000,
                            PostingType type = pt_default,
                            autil::mem_pool::Pool *sessionPool = NULL) 
    {
        std::map<const std::string, docid_t>::const_iterator it = _map.find(term.GetWord());
        if(it != _map.end()){
            return POOL_COMPATIBLE_NEW_CLASS(sessionPool, PrimaryKeyPostingIterator, it->second, sessionPool);
        }
        return NULL;
    }

    void setAttributeValues(const std::string &values) {_values = values;}
    void setPKIndexString( const std::string &mapStr) {convertStringToMap(mapStr);}
    void SetDeletionMapReader(const DeletionMapReaderPtr &delReader) 
    { this->mDeletionMapReader = delReader; }

    void LoadFakePrimaryKeyIndex(const std::string &fakePkString)
    {
        //key1:doc_id1,key2:doc_id2;key3:doc_id3,key4:doc_id4
        std::stringstream ss;
        ss << "../pk_temp_path_" << pthread_self() << "/";
        std::string path = ss.str();
        if (HA3_NS(util)::FileUtil::localDirExist(path)) {
            HA3_NS(util)::FileUtil::removeLocalDir(path, true);
        }
        HA3_NS(util)::FileUtil::makeLocalDir(path);
        _rootDirectory = IndexDirectoryCreator::Create(path);

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
                this->Hash(kvPair[0], pkPair.key);
                pkPair.docid = autil::StringUtil::fromString<docid_t>(kvPair[1]);
                pairVec.push_back(pkPair);
            }
            std::sort(pairVec.begin(), pairVec.end());
            pushDataToSegmentList(pairVec, baseDocId, (segmentid_t)x);
            baseDocId += kvVector.size();
        }

        HA3_NS(util)::FileUtil::removeLocalDir(path, true);
    }

private:
    void pushDataToSegmentList(const PKPairVec& pairVec, 
                               docid_t baseDocId, 
                               segmentid_t segmentId)
    {
        std::stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId;
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
        PrimaryKeyLoadPlanPtr plan(new PrimaryKeyLoadPlan());
        plan->Init(baseDocId, pkIndexConfig);
        plan->AddSegmentData(segData, 0);
        file_system::FileReaderPtr fileReader = 
            PrimaryKeyLoader<T>::Load(plan, pkIndexConfig);
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
            _values += autil::StringUtil::toString(i) + ",";
        }
    }


private:
    std::string _values;
    std::map<const std::string, docid_t> _map;
    file_system::DirectoryPtr _rootDirectory;
};

typedef FakePrimaryKeyReader<uint64_t> UInt64FakePrimaryKeyReader;
typedef std::tr1::shared_ptr<UInt64FakePrimaryKeyReader> UInt64FakePrimaryKeyReaderPtr;

typedef FakePrimaryKeyReader<autil::uint128_t> UInt128FakePrimaryKeyReader;
typedef std::tr1::shared_ptr<UInt128FakePrimaryKeyReader> UInt128FakePrimaryKeyReaderPtr;

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEPRIMARYKEYREADER_H
