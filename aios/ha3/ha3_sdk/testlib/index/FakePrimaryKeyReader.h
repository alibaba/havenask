#pragma once

#include "autil/Log.h" // IWYU pragma: keep
#include "fslib/util/FileUtil.h"
#include "ha3/isearch.h"
#include "ha3_sdk/testlib/index/FakeAttributeReader.h"
#include "ha3_sdk/testlib/index/IndexDirectoryCreator.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyHashConvertor.h"

namespace indexlib {
namespace index {

template <typename T>
class FakePrimaryKeyReader : public LegacyPrimaryKeyReader<T> {
public:
    using SegmentReader = indexlibv2::index::PrimaryKeyDiskIndexer<T>;
    using PKPairTyped = indexlibv2::index::PKPair<T, docid_t>;
    using PKPairVec = std::vector<PKPairTyped>;
    using SegmentReaderPtr = std::shared_ptr<SegmentReader>;

public:
    FakePrimaryKeyReader() {
        this->_fieldType = ft_string;
        this->_primaryKeyHashType = pk_default_hash;

        config::PrimaryKeyIndexConfigPtr pkIndexConfig(new config::PrimaryKeyIndexConfig(
            "primaryKey", (typeid(T) == typeid(uint64_t)) ? it_primarykey64 : it_primarykey128));
        pkIndexConfig->SetPrimaryKeyIndexType(pk_sort_array);
        this->_indexConfig = pkIndexConfig;
    }

    ~FakePrimaryKeyReader() {}

public:
    AttributeReaderPtr GetLegacyPKAttributeReader() const {
        FakeAttributeReader<T> *attrReader = new FakeAttributeReader<T>();
        attrReader->setAttributeValues(_values);
        return AttributeReaderPtr(attrReader);
    }

    index::Result<PostingIterator *> Lookup(const Term &term,
                                            uint32_t statePoolSize = 1000,
                                            PostingType type = pt_default,
                                            autil::mem_pool::Pool *sessionPool = NULL) {
        std::map<const std::string, docid_t>::const_iterator it = _map.find(term.GetWord());
        if (it != _map.end()) {
            return POOL_COMPATIBLE_NEW_CLASS(
                sessionPool, PrimaryKeyPostingIterator, it->second, sessionPool);
        }
        return NULL;
    }

    void setAttributeValues(const std::string &values) {
        _values = values;
    }
    void setPKIndexString(const std::string &mapStr) {
        convertStringToMap(mapStr);
    }
    void SetDeletionMapReader(const DeletionMapReaderPtr &delReader) {
        this->_deletionMapReader = delReader;
    }

    void LoadFakePrimaryKeyIndex(const std::string &fakePkString) {
        // key1:doc_id1,key2:doc_id2;key3:doc_id3,key4:doc_id4
        std::stringstream ss;
        ss << "../pk_temp_path_" << pthread_self() << "/";
        std::string path = ss.str();
        if (fslib::util::FileUtil::isExist(path)) {
            fslib::util::FileUtil::remove(path);
        }
        fslib::util::FileUtil::mkDir(path);
        _rootDirectory = IndexDirectoryCreator::Create(path);

        std::vector<std::string> segs = autil::StringTokenizer::tokenize(
            autil::StringView(fakePkString),
            ";",
            autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        docid_t baseDocId = 0;
        for (size_t x = 0; x < segs.size(); ++x) {
            PKPairVec pairVec;
            std::vector<std::string> kvVector = autil::StringUtil::split(segs[x], ",", true);
            for (size_t i = 0; i < kvVector.size(); i++) {
                std::vector<std::string> kvPair = autil::StringUtil::split(kvVector[i], ":", true);
                assert(kvPair.size() == 2);
                PKPairTyped pkPair;
                this->Hash(kvPair[0], pkPair.key);
                pkPair.docid = autil::StringUtil::fromString<docid_t>(kvPair[1]);
                pairVec.push_back(pkPair);
            }
            std::sort(pairVec.begin(), pairVec.end());
            pushDataToSegmentList(pairVec, baseDocId, (segmentid_t)x);
            baseDocId += kvVector.size();
        }

        fslib::util::FileUtil::remove(path);
    }

private:
    void pushDataToSegmentList(const PKPairVec &pairVec, docid_t baseDocId, segmentid_t segmentId) {
        std::stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId;
        file_system::DirectoryPtr segmentDir
            = _rootDirectory->MakeDirectory(ss.str(), file_system::DirectoryOption::Mem());
        file_system::DirectoryPtr primaryKeyDir
            = segmentDir->MakeDirectory(std::string(INDEX_DIR_NAME) + "/primaryKey");

        file_system::FileWriterPtr fileWriter
            = primaryKeyDir->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME);
        assert(fileWriter);
        for (size_t i = 0; i < pairVec.size(); i++) {
            fileWriter->Write((void *)&pairVec[i], sizeof(PKPairTyped)).GetOrThrow();
        }
        fileWriter->Close().GetOrThrow();

        config::PrimaryKeyIndexConfigPtr pkIndexConfig = createIndexConfig();
        indexlibv2::index::IndexerParameter parameter;
        parameter.docCount = pairVec.size();
        SegmentReaderPtr segReader(new SegmentReader(parameter));
        std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> pkIndexConfigV2(
            pkIndexConfig->MakePrimaryIndexConfigV2());
        auto status = segReader->Open(pkIndexConfigV2, primaryKeyDir->GetIDirectory());
        assert(status.IsOK());
        this->_segmentReaderList.push_back(
            {std::make_pair(baseDocId, segReader), std::vector<segmentid_t> {segmentId}});
    }

    config::PrimaryKeyIndexConfigPtr createIndexConfig() {
        return DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->_indexConfig);
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
typedef std::shared_ptr<UInt64FakePrimaryKeyReader> UInt64FakePrimaryKeyReaderPtr;

typedef FakePrimaryKeyReader<autil::uint128_t> UInt128FakePrimaryKeyReader;
typedef std::shared_ptr<UInt128FakePrimaryKeyReader> UInt128FakePrimaryKeyReaderPtr;

} // namespace index
} // namespace indexlib
