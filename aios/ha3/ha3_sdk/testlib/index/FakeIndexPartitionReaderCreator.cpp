#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "autil/ConstString.h"
#include "autil/MultiValueType.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/util/FileUtil.h"
#include "future_lite/Common.h"
#include "future_lite/Future.h"
#include "future_lite/Try.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartialIndexPartitionReaderWrapper.h"
#include "ha3_sdk/testlib/index/FakeBitmapIndexReader.h"
#include "ha3_sdk/testlib/index/FakeIndexPartitionReader.h"
#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderWrapper.h"
#include "ha3_sdk/testlib/index/FakeIndexReader.h"
#include "ha3_sdk/testlib/index/FakePartialIndexPartitionReaderWrapper.h"
#include "ha3_sdk/testlib/index/FakeSummaryReader.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/locator.h"
#include "indexlib/index/normal/attribute/accessor/primary_key_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/partition_info_holder.h"
#include "indexlib/util/Exception.h"
using namespace fslib::util;
using namespace std;
using namespace autil;
using namespace indexlib::document;
using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace isearch::search;

namespace indexlib {
namespace index {

FakeIndexPartitionReaderCreator::FakeIndexPartitionReaderCreator() {}

FakeIndexPartitionReaderCreator::~FakeIndexPartitionReaderCreator() {}

IndexPartitionReaderWrapperPtr
FakeIndexPartitionReaderCreator::createIndexPartitionReader(const FakeIndex &fakeIndex,
                                                            indexlib::index::ErrorCode seekRet) {
    FakeIndexPartitionReader *fakePartitionReader = new FakeIndexPartitionReader();
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("mainTable"));
    fakePartitionReader->SetSchema(schema);
    fakePartitionReader->setIndexVersion(Version(fakeIndex.versionId));
    string segmentDocCounts = fakeIndex.segmentDocCounts;
    if (segmentDocCounts.empty()) {
        segmentDocCounts = "10000";
    }
    initSegmentDocCounts(fakePartitionReader, segmentDocCounts);

    // create deletion map reader
    DeletionMapReaderPtr delReader
        = createDeletionMapReader(fakeIndex.deletedDocIds, fakePartitionReader->getDocCount());
    fakePartitionReader->SetDeletionMapReader(delReader);

    // create summary reader
    createFakeSummaryReader(fakePartitionReader, fakeIndex.summarys);

    map<string, uint32_t> prefixToIdx;
    vector<FakeIndexPartitionReader *> indexPartReaders;
    const vector<string> &tablePrefix = fakeIndex.tablePrefix;
    indexPartReaders.push_back(fakePartitionReader);
    for (size_t i = 0; i < tablePrefix.size(); ++i) {
        prefixToIdx[tablePrefix[i]] = indexPartReaders.size();
        indexPartReaders.push_back(new FakeIndexPartitionReader);
    }

    map<string, uint32_t> *indexName2IdMap = new map<string, uint32_t>();
    map<string, uint32_t> *attrName2IdMap = new map<string, uint32_t>();

    initIndexPartReaders(
        fakeIndex, prefixToIdx, indexPartReaders, *indexName2IdMap, *attrName2IdMap, seekRet);
    // create partition meta
    PartitionMetaPtr meta(new PartitionMeta);
    StringTokenizer st1(fakeIndex.partitionMeta,
                        ",",
                        StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st1.getNumTokens(); ++i) {
        if (st1[i][0] == '+') {
            meta->AddSortDescription(st1[i].substr(1), indexlibv2::config::sp_asc);
        } else if (st1[i][0] == '-') {
            meta->AddSortDescription(st1[i].substr(1), indexlibv2::config::sp_desc);
        }
    }
    fakePartitionReader->setPartitionMeta(meta);
    fakePartitionReader->initSortedDocidRangeSearcher();

    vector<IndexPartitionReaderPtr> *indexPartReaderPtrs = new vector<IndexPartitionReaderPtr>();
    for (size_t i = 0; i < indexPartReaders.size(); ++i) {
        indexPartReaderPtrs->push_back(IndexPartitionReaderPtr(indexPartReaders[i]));
    }
    if (indexPartReaderPtrs->size() > 1) {
        // TODO:
        fakePartitionReader->SetSubIndexPartitionReader(
            (*indexPartReaderPtrs)[IndexPartitionReader::SUB_PART_ID]);
    }
    IndexPartitionReaderWrapperPtr wrapper(new FakeIndexPartitionReaderWrapper(
        indexName2IdMap, attrName2IdMap, indexPartReaderPtrs, true));
    return wrapper;
}

PartialIndexPartitionReaderWrapperPtr
FakeIndexPartitionReaderCreator::createPartialIndexPartitionReader(
    const FakeIndex &fakeIndex, indexlib::index::ErrorCode seekRet) {
    FakeIndexPartitionReader *fakePartitionReader = new FakeIndexPartitionReader();
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("mainTable"));
    fakePartitionReader->SetSchema(schema);
    fakePartitionReader->setIndexVersion(Version(fakeIndex.versionId));
    string segmentDocCounts = fakeIndex.segmentDocCounts;
    if (segmentDocCounts.empty()) {
        segmentDocCounts = "10000";
    }
    initSegmentDocCounts(fakePartitionReader, segmentDocCounts);

    // create deletion map reader
    DeletionMapReaderPtr delReader
        = createDeletionMapReader(fakeIndex.deletedDocIds, fakePartitionReader->getDocCount());
    fakePartitionReader->SetDeletionMapReader(delReader);

    // create summary reader
    createFakeSummaryReader(fakePartitionReader, fakeIndex.summarys);

    map<string, uint32_t> prefixToIdx;
    vector<FakeIndexPartitionReader *> indexPartReaders;
    const vector<string> &tablePrefix = fakeIndex.tablePrefix;
    indexPartReaders.push_back(fakePartitionReader);
    for (size_t i = 0; i < tablePrefix.size(); ++i) {
        prefixToIdx[tablePrefix[i]] = indexPartReaders.size();
        indexPartReaders.push_back(new FakeIndexPartitionReader);
    }

    map<string, uint32_t> *indexName2IdMap = new map<string, uint32_t>();
    map<string, uint32_t> *attrName2IdMap = new map<string, uint32_t>();
    initIndexPartReaders(
        fakeIndex, prefixToIdx, indexPartReaders, *indexName2IdMap, *attrName2IdMap, seekRet);
    // create partition meta
    PartitionMetaPtr meta(new PartitionMeta);
    StringTokenizer st1(fakeIndex.partitionMeta,
                        ",",
                        StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st1.getNumTokens(); ++i) {
        if (st1[i][0] == '+') {
            meta->AddSortDescription(st1[i].substr(1), indexlibv2::config::sp_asc);
        } else if (st1[i][0] == '-') {
            meta->AddSortDescription(st1[i].substr(1), indexlibv2::config::sp_desc);
        }
    }
    fakePartitionReader->setPartitionMeta(meta);
    fakePartitionReader->initSortedDocidRangeSearcher();

    vector<IndexPartitionReaderPtr> *indexPartReaderPtrs = new vector<IndexPartitionReaderPtr>();
    for (size_t i = 0; i < indexPartReaders.size(); ++i) {
        indexPartReaderPtrs->push_back(IndexPartitionReaderPtr(indexPartReaders[i]));
    }
    if (indexPartReaderPtrs->size() > 1) {
        // TODO:
        fakePartitionReader->SetSubIndexPartitionReader(
            (*indexPartReaderPtrs)[IndexPartitionReader::SUB_PART_ID]);
    }
    PartialIndexPartitionReaderWrapperPtr wrapper(new FakePartialIndexPartitionReaderWrapper(
        indexName2IdMap, attrName2IdMap, indexPartReaderPtrs, true));
    return wrapper;
}

void FakeIndexPartitionReaderCreator::initSegmentDocCounts(FakeIndexPartitionReader *reader,
                                                           const string &segDocCountsStr) {
    vector<string> segDocCounts
        = StringTokenizer::tokenize(StringView(segDocCountsStr), "|", StringTokenizer::TOKEN_TRIM);
    assert(segDocCounts.size() == 1 || segDocCounts.size() == 2);
    vector<string> orderedSegDocCounts = StringTokenizer::tokenize(
        StringView(segDocCounts[0]),
        ",",
        StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);

    vector<docid_t> baseDocIds;
    indexlib::DocIdRangeVector orderedRanges;
    uint32_t segmentCount = orderedSegDocCounts.size();
    docid_t baseDocId = 0;
    for (size_t i = 0; i < orderedSegDocCounts.size(); ++i) {
        baseDocIds.push_back(baseDocId);
        uint32_t docCount = StringUtil::fromString<uint32_t>(orderedSegDocCounts[i].c_str());
        orderedRanges.push_back(indexlib::DocIdRange(baseDocId, baseDocId + docCount));
        baseDocId += docCount;
    }

    reader->setIncDocCount(baseDocId);
    reader->SetOrderedDocIds(orderedRanges);
    reader->SetLastIncSegmentId((segmentid_t)baseDocIds.size());
    if (segDocCounts.size() == 2) {
        baseDocIds.push_back(baseDocId);
        uint32_t docCount = StringUtil::fromString<uint32_t>(segDocCounts[1]);
        reader->SetUnOrderedDocIdRange(indexlib::DocIdRange(baseDocId, baseDocId + docCount));
        baseDocId += docCount;
        segmentCount++;
    }
    reader->SetBaseDocIds(baseDocIds);
    reader->setDocCount(baseDocId);
    reader->setSegmentCount(segmentCount);
}

void FakeIndexPartitionReaderCreator::createFakeAttributeReader(
    FakeIndexPartitionReader *fakePartitionReader,
    const string &attrName,
    const string &type,
    const string &values) {
    if (attrName == "main_docid_to_sub_docid_attr_name"
        || attrName == "sub_docid_to_main_docid_attr_name") {
        assert(type == "int32_t");
        fakePartitionReader->createFakeJoinDocidAttributeReader(attrName, values);
        return;
    }

    if (type == "int8_t") {
        fakePartitionReader->createFakeAttributeReader<int8_t>(attrName, values);
    } else if (type == "uint8_t") {
        fakePartitionReader->createFakeAttributeReader<uint8_t>(attrName, values);
    } else if (type == "int16_t") {
        fakePartitionReader->createFakeAttributeReader<int16_t>(attrName, values);
    } else if (type == "uint16_t") {
        fakePartitionReader->createFakeAttributeReader<uint16_t>(attrName, values);
    } else if (type == "int32_t") {
        fakePartitionReader->createFakeAttributeReader<int32_t>(attrName, values);
    } else if (type == "uint32_t") {
        fakePartitionReader->createFakeAttributeReader<uint32_t>(attrName, values);
    } else if (type == "int64_t") {
        fakePartitionReader->createFakeAttributeReader<int64_t>(attrName, values);
    } else if (type == "uint64_t") {
        fakePartitionReader->createFakeAttributeReader<uint64_t>(attrName, values);
    } else if (type == "float") {
        fakePartitionReader->createFakeAttributeReader<float>(attrName, values);
    } else if (type == "double") {
        fakePartitionReader->createFakeAttributeReader<double>(attrName, values);
    } else if (type == "string") {
        fakePartitionReader->createFakeStringAttributeReader(attrName, values);
    } else if (type == "multi_int8_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int8_t>(attrName, values);
    } else if (type == "multi_uint8_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint8_t>(attrName, values);
    } else if (type == "multi_int16_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int16_t>(attrName, values);
    } else if (type == "multi_uint16_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint16_t>(attrName, values);
    } else if (type == "multi_int32_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int32_t>(attrName, values);
    } else if (type == "multi_uint32_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint32_t>(attrName, values);
    } else if (type == "multi_int64_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int64_t>(attrName, values);
    } else if (type == "multi_uint64_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint64_t>(attrName, values);
    } else if (type == "multi_float") {
        fakePartitionReader->createFakeMultiAttributeReader<float>(attrName, values);
    } else if (type == "multi_double") {
        fakePartitionReader->createFakeMultiAttributeReader<double>(attrName, values);
    } else if (type == "multi_string") {
        fakePartitionReader->createFakeMultiAttributeReader<MultiChar>(attrName, values);
    }
}

void FakeIndexPartitionReaderCreator::createFakeSummaryReader(
    FakeIndexPartitionReader *fakePartitionReader, const string &summaryStr) {
    vector<vector<string>> summaryDocFields;
    StringUtil::fromString(summaryStr, summaryDocFields, ",", ";");
    FakeSummaryReader *fakeSummaryReader = new FakeSummaryReader;
    for (size_t i = 0; i < summaryDocFields.size(); i++) {
        SearchSummaryDocument *doc = new SearchSummaryDocument(NULL, summaryDocFields[i].size());
        for (size_t j = 0; j < summaryDocFields[i].size(); j++) {
            doc->SetFieldValue(
                (int32_t)j, summaryDocFields[i][j].c_str(), summaryDocFields[i][j].size());
        }
        fakeSummaryReader->AddSummaryDocument(doc);
    }
    fakePartitionReader->SetSummaryReader(SummaryReaderPtr(fakeSummaryReader));
}

DeletionMapReaderPtr
FakeIndexPartitionReaderCreator::createDeletionMapReader(const string &deletedDocIdStr,
                                                         uint32_t totalDocCount) {
    vector<docid_t> deletedDocIds;
    StringUtil::fromString(deletedDocIdStr, deletedDocIds, ",");
    DeletionMapReaderPtr delReaderPtr(new DeletionMapReader(totalDocCount));
    for (size_t i = 0; i < deletedDocIds.size(); i++) {
        delReaderPtr->Delete(deletedDocIds[i]);
    }
    return delReaderPtr;
}

void FakeIndexPartitionReaderCreator::initIndexPartReaders(
    const FakeIndex &fakeIndex,
    const map<string, uint32_t> &prefixToIdx,
    const vector<FakeIndexPartitionReader *> &indexPartVec,
    map<string, uint32_t> &indexName2IdMap,
    map<string, uint32_t> &attrName2IdMap,
    indexlib::index::ErrorCode seekRet) {
    // create index reader...
    vector<FakeIndexReader *> indexReaders;
    for (size_t i = 0; i < indexPartVec.size(); ++i) {
        indexReaders.push_back(new FakeIndexReader);
        indexPartVec[i]->SetIndexReader(LegacyIndexReaderPtr(indexReaders[i]));
    }
    for (std::map<std::string, std::string>::const_iterator it = fakeIndex.indexes.begin();
         it != fakeIndex.indexes.end();
         ++it) {
        const string &indexName = it->first;
        string identifier;
        uint32_t idx = getIndexPartReaderIdx(prefixToIdx, indexName, identifier);
        indexName2IdMap[indexName] = idx;
        auto indexReader = indexReaders[idx];
        IndexConfigPtr config(new SingleFieldIndexConfig(indexName, it_text));
        if (identifier == "pk") {
            indexPartVec[idx]->createFakePrimaryKeyReader<uint64_t>("", it->second);
        } else if (identifier.length() >= 6 && identifier.substr(0, 6) == "bitmap") {
            indexReader->SetIndexConfig(config);
            indexReader->addIndexReader(indexName, new FakeBitmapIndexReader(it->second));
        } else {
            indexReader->SetIndexConfig(config);
            indexReader->addIndexReader(indexName, new FakeTextIndexReader(it->second, seekRet));
        }
    }

    // create attribute reader
    vector<string> attributes = StringTokenizer::tokenize(StringView(fakeIndex.attributes),
                                                          ";",
                                                          StringTokenizer::TOKEN_IGNORE_EMPTY
                                                              | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < attributes.size(); ++i) {
        vector<string> attrItems = StringTokenizer::tokenize(StringView(attributes[i]),
                                                             ":",
                                                             StringTokenizer::TOKEN_IGNORE_EMPTY
                                                                 | StringTokenizer::TOKEN_TRIM);
        assert(attrItems.size() == 3);
        string identifier;
        uint32_t idx = getIndexPartReaderIdx(prefixToIdx, attrItems[0], identifier);
        string attrName = attrItems[0];
        // remove prefix of sub join cache
        if (idx == 1) {
            size_t pos
                = attrItems[0].find(partition::BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX);
            if (pos != string::npos) {
                attrName = attrItems[0].substr(pos);
            }
        }
        attrName2IdMap[attrName] = idx;
        createFakeAttributeReader(indexPartVec[idx], attrName, attrItems[1], attrItems[2]);
    }
}

uint32_t FakeIndexPartitionReaderCreator::getIndexPartReaderIdx(
    const map<string, uint32_t> &prefixToIdx, const string &orgIdentifier, string &newIdentifier) {
    for (map<string, uint32_t>::const_iterator it = prefixToIdx.begin(); it != prefixToIdx.end();
         ++it) {
        if (orgIdentifier.find(it->first) == 0) {
            newIdentifier = orgIdentifier.substr(it->first.size());
            return it->second;
        }
    }
    newIdentifier = orgIdentifier;
    return 0;
}

} // namespace index
} // namespace indexlib
