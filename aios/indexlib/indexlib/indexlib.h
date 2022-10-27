#ifndef __INDEX_ENGINE_H
#define __INDEX_ENGINE_H
#include <stdint.h>
#include <sstream>
#include <assert.h>
#include <vector>
#include <autil/MultiValueType.h>
#include <limits>

typedef int32_t docid_t;
typedef int64_t exdocid_t;
typedef int64_t globalid_t;
typedef int32_t fieldid_t;
typedef int16_t payloadid_t;

typedef uint32_t field_len_t;

typedef int32_t df_t;
typedef int32_t tf_t;
typedef int64_t ttf_t;
typedef uint32_t pos_t;
typedef uint8_t fieldmap_t;
typedef uint8_t firstocc_t;
typedef uint16_t docpayload_t;
typedef uint8_t pospayload_t;
typedef uint32_t termpayload_t;
typedef int32_t segmentid_t;
typedef int32_t versionid_t;

typedef uint8_t optionflag_t;
typedef uint8_t token_len_t;

typedef uint64_t dictkey_t;
typedef int64_t dictvalue_t;

typedef int32_t interfaceid_t;

typedef uint16_t segmentindex_t;
static constexpr size_t MAX_SPLIT_SEGMENT_COUNT = 1lu << 16;


const dictvalue_t INVALID_DICT_VALUE = (dictvalue_t)-1; 
const uint8_t MAX_DICT_INLINE_AVAILABLE_SIZE = 7;
const uint8_t BIG_ENDIAN_DICT_INLINE_SHIFT_BITNUM = 8;
const int64_t DEFAULT_MEMORY_USE_LIMIT = (int64_t)1024 * 1024; // 1T (unit MB)
const size_t DEFAULT_MAX_OPERATION_BLOCK_SIZE = (size_t)1024 * 1024; // 1M operation
// default time to live in seconds, make sure not overflow when converted to microsecond
const int64_t DEFAULT_TIME_TO_LIVE = std::numeric_limits<int64_t>::max() >> 20;

namespace heavenask { namespace indexlib {

enum MatchValueType
{
    mv_int8,
    mv_uint8,
    mv_int16,
    mv_uint16,
    mv_int32,
    mv_uint32,
    mv_float,
    mv_unknown
};

class MatchValue {
public:
    int8_t GetInt8(){ return int8; };
    void SetInt8(int8_t value){ int8 = value; };
    uint8_t GetUint8(){ return uint8; };
    void SetUint8(uint8_t value){ uint8 = value; };
    int16_t GetInt16(){ return int16; };
    void SetInt16(int16_t value){ int16 = value; };
    uint16_t GetUint16(){ return uint16; };
    void SetUint16(uint16_t value){ uint16 = value; };
    int32_t GetInt32(){ return int32; };
    void SetInt32(int32_t value){ int32 = value; };
    uint32_t GetUint32(){ return uint32; };
    void SetUint32(uint32_t value){ uint32 = value; };
    float GetFloat(){ return float32; };
    void SetFloat(float value){ float32 = value; };
private:
    union {
        int8_t int8;
        uint8_t uint8;
        int16_t int16;
        uint16_t uint16;
        int32_t int32;
        uint32_t uint32;
        float float32;
    };
};

#pragma pack(push)
#pragma pack(1)

#define BYTE_TYPE(num)                              \
        struct byte##num##_t {                      \
            uint64_t data : num*8;                  \
        };
BYTE_TYPE(1);
BYTE_TYPE(2);
BYTE_TYPE(3);        
BYTE_TYPE(4);
BYTE_TYPE(5);
BYTE_TYPE(6);
BYTE_TYPE(7);
BYTE_TYPE(8);        
#pragma pack(pop)

}}

typedef heavenask::indexlib::MatchValue matchvalue_t;


typedef uint16_t tokenattr_t;
typedef int64_t timestamp_t;

enum AttrType
{
    AT_INT32 = 1,
    AT_UINT32,
    AT_INT64,
    AT_UINT64,
    AT_INT8,
    AT_UINT8,
    AT_INT16,
    AT_UINT16,
    AT_FLOAT,
    AT_DOUBLE,
    AT_STRING,
    AT_HASH_64,
    AT_HASH_128,

    AT_VIRTUAL_INT16,
    AT_VIRTUAL_INT32,
    AT_VIRTUAL_INT64,
    AT_VIRTUAL_UINT16,
    AT_VIRTUAL_UINT32,
    AT_VIRTUAL_UINT64,

    AT_UNKNOWN
};

#define INVALID_VERSION ((versionid_t)-1)
#define INVALID_TIMESTAMP ((int64_t)-1)
#define INVALID_DOC_VERSION ((uint32_t)-1)

#define INVALID_DOCID ((docid_t)-1)
#define UNINITIALIZED_DOCID ((docid_t)-2)
#define INVALID_GLOBALID ((globalid_t)-1)
#define INVALID_POS_PAYLOAD ((pospayload_t)0)
#define INVALID_DOC_PAYLOAD ((docpayload_t)0)
#define INVALID_TF ((tf_t)-1)
#define INVALID_TTF ((ttf_t)-1)
#define INVALID_DF ((df_t)-1)
#define INVALID_SEGMENTID ((segmentid_t)-1)
#define DEFAULT_TOKEN_ATTRIBUTE 0
#define DEFAULT_INTERFACE_ID ((interfaceid_t)0)

typedef std::pair<docid_t, docid_t> DocIdRange;
typedef std::vector<DocIdRange> DocIdRangeVector;

const int64_t MAX_FREE_MEMORY = std::numeric_limits<int64_t>::max();

const docid_t MAX_DOCID = std::numeric_limits<docid_t>::max() - 1;

const pos_t INVALID_POSITION = std::numeric_limits<pos_t>::max();

const fieldid_t INVALID_FIELDID = (fieldid_t)-1;
const payloadid_t INVALID_PAYLOADID = (payloadid_t)-1;

typedef uint32_t summaryid_t;
const summaryid_t INVALID_SUMMARYID = (summaryid_t)-1;

typedef uint32_t indexid_t;
const indexid_t INVALID_INDEXID = (indexid_t)-1;

typedef uint32_t attrid_t;
const attrid_t INVALID_ATTRID = (attrid_t)-1;

typedef uint32_t packattrid_t;
const packattrid_t INVALID_PACK_ATTRID = (packattrid_t)-1;

typedef uint32_t analyzerid_t;
const analyzerid_t INVALID_ANALYZERID = (analyzerid_t)-1;

typedef uint16_t sectionid_t;
const sectionid_t INVALID_SECID = (sectionid_t)-1;

typedef int32_t summaryfieldid_t;
const summaryfieldid_t INVALID_SUMMARYFIELDID = (summaryfieldid_t)-1;

typedef int32_t summarygroupid_t;
const summarygroupid_t INVALID_SUMMARYGROUPID = (summarygroupid_t)-1;
const summarygroupid_t DEFAULT_SUMMARYGROUPID = 0;
static const std::string DEFAULT_SUMMARYGROUPNAME =  "__DEFAULT_SUMMARYGROUPNAME__";
static const std::string DOCUMENT_SOURCE_TAG_KEY = "__source__";
static const std::string DOCUMENT_HASHID_TAG_KEY = "__hash_id__";
static const std::string DOCUMENT_HASHFIELD_TAG_KEY = "__hash_field__";
static const std::string DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY = "__ingestion_timestamp__";
typedef std::vector<summarygroupid_t> SummaryGroupIdVec;

typedef int32_t regionid_t;
const regionid_t INVALID_REGIONID = (regionid_t)-1;
const regionid_t DEFAULT_REGIONID = (regionid_t)0;
static const std::string DEFAULT_REGIONNAME =  "__DEFAULT_INDEX_REGION__";

typedef uint32_t schemavid_t;
const schemavid_t DEFAULT_SCHEMAID = (schemavid_t)0;

typedef uint32_t schema_opid_t; // begin from 1
const schema_opid_t INVALID_SCHEMA_OP_ID = (schema_opid_t)0;

typedef uint8_t section_fid_t;
typedef uint16_t section_len_t;
typedef uint16_t section_weight_t;

const uint32_t INVALID_KEEP_VERSION_COUNT = -1;

const int32_t  INVALID_INT32_ATTRIBUTE = 0xFFFFFFFE;
const uint32_t INVALID_STRUCT_PAYLOAD_LEN = 0x0000FFFF;

const uint32_t ALL_FIELDBITS = 0;
const uint32_t MAX_FIELD_BIT_COUNT = 8;
const uint32_t MAX_DOC_NUM_PER_FUXI_PARTITION = 50 * 1024 * 1024;

const uint32_t MAX_DOC_PER_RECORD = 128;
const uint32_t MAX_DOC_PER_RECORD_BIT_NUM = 7;
const uint32_t MAX_POS_PER_RECORD = 128;
const uint32_t MAX_POS_PER_RECORD_BIT_NUM = 7;
const uint32_t MAX_POS_PER_RECORD_MASK = MAX_POS_PER_RECORD - 1;
const uint32_t MAX_DOC_PER_BITMAP_BLOCK = 256;
const uint32_t MAX_DOC_PER_BITMAP_BLOCK_BIT_NUM = 8;

const uint32_t SEGMENT_DOC_COUNT_PACK = 1024;
const uint32_t SEGMENT_PACK_POWER = 10;                           // used for bit >>
const uint32_t SEGMENT_PACK_MARK = (SEGMENT_DOC_COUNT_PACK - 1);  // used for bit &

const uint32_t POSTING_TABLE_SIZE_DEF = 786433;
const uint32_t TOKEN_NUM_PER_DOC_DEF = 3000;

const uint64_t CACHE_SIZE_DEF = 1 << 28; // default cache size in bytes
const double CACHE_SIZE_RATIO_DEF = 0.1; // default cache size in ratio

const std::string LOAD_CONFIG_INDEX_NAME = "index";
const std::string LOAD_CONFIG_ATTRIBUTE_NAME = "attribute";
const std::string LOAD_CONFIG_SUMMARY_NAME = "summary";

const std::string READ_MODE_MMAP = "mmap";
const std::string READ_MODE_CACHE = "cache";
const std::string READ_MODE_GLOBAL_CACHE = "global_cache";
const std::string READ_MODE_IN_MEM = "inmem";

const char MULTI_VALUE_SEPARATOR = autil::MULTI_VALUE_DELIMITER;
const std::string INDEXLIB_MULTI_VALUE_SEPARATOR_STR = std::string(1, MULTI_VALUE_SEPARATOR);

enum FieldType 
{
    ft_text,
    ft_string,
    ft_enum,
    ft_integer,
    ft_int32 = ft_integer,
    ft_float,
    ft_long,
    ft_int64 = ft_long,
    ft_time,
    ft_location,
    ft_polygon,
    ft_line,
    ft_online,
    ft_property,
    ft_uint32,
    ft_uint64,
    ft_hash_64,            // uint64,  only used for primary key
    ft_hash_128,           // uint128, only used for primary key
    ft_int8,
    ft_uint8,
    ft_int16,
    ft_uint16,
    ft_double,
    ft_raw,
    ft_unknown,
    ft_byte1,
    ft_byte2,
    ft_byte3,
    ft_byte4,
    ft_byte5,
    ft_byte6,
    ft_byte7,
    ft_byte8,
    ft_fp8,
    ft_fp16
};

const size_t FIELD_SORTABLE_COUNT = 14;
const FieldType FIELD_SORTABLE_TYPE[FIELD_SORTABLE_COUNT] = 
{
    ft_integer,
    ft_float,
    ft_long,
    ft_uint32,
    ft_uint64,
    ft_hash_64,            
    ft_hash_128,           
    ft_int8,
    ft_uint8,
    ft_int16,
    ft_uint16,
    ft_double,
    ft_fp8,
    ft_fp16
};


enum SupportLanguage
{
    sl_English,
    sl_Chinese,
    sl_Traditional,
    sl_Japanese,
    sl_Korean,
    sl_unknown
};

enum SupportCoding
{
    sc_UTF8,
    sc_Unicode,
    sc_Big5,
    sc_GBK,
    sc_Ansi
};

enum IndexType 
{
    it_text,
    it_pack,
    it_expack,
    it_string,
    it_enum,
    it_property,
    it_number,  // use external -- legacy
    it_number_int8,  // 8 - 64 use internal, type transform in InitIndexWriters
    it_number_uint8,
    it_number_int16,
    it_number_uint16,
    it_number_int32,
    it_number_uint32,
    it_number_int64,
    it_number_uint64,
    it_primarykey64,
    it_primarykey128,
    it_trie,
    it_spatial,
    it_kv,
    it_kkv,
    it_customized,
    it_date,
    it_range,
    it_unknown
};

enum IndexExtend 
{
    ie_none = 0,
    ie_occ_none = 1,
    ie_occ_array = 2,
    ie_occ_first = 4,
    ie_int = 8,
    ie_pk = 16,
    ie_md5 = 32,
    ie_double = 64,
    ie_range = 128,
    ie_wildcard = 256,
    ie_cache = 512,
    ie_stopword_ignore = 1024,
};

enum SegmentType 
{
    st_normal,
    st_merge,
};

enum OptionFlag 
{
    of_none             = 0,  
    of_term_payload     = 1, // 1 << 0
    of_doc_payload      = 2, // 1 << 1
    of_position_payload = 4, // 1 << 2
    of_position_list    = 8, // 1 << 3
    of_term_frequency   = 16,// 1 << 4
    of_tf_bitmap        = 32, // 1 << 5
    of_fieldmap         = 64, // 1 << 6
};

enum PostingType
{
    pt_default,
    pt_normal,
    pt_bitmap,
};

enum PostingIteratorType
{
    pi_pk,
    pi_bitmap,
    pi_buffered,
    pi_spatial,
    pi_customized,
    pi_range,
    pi_seek_and_filter,
    pi_unknown,
};

enum HighFrequencyTermPostingType
{
    hp_bitmap,
    hp_both
};

enum SortPattern
{
    sp_nosort,
    sp_asc,
    sp_desc
};

enum DocOperateType
{
    UNKNOWN_OP = 0,
    ADD_DOC = 1,
    DELETE_DOC = 2,
    UPDATE_FIELD = 4,
    SKIP_DOC = 8,
    DELETE_SUB_DOC = 16
};

enum PrimaryKeyIndexType
{
    pk_sort_array,
    pk_hash_table
};

enum PrimaryKeyHashType
{
    pk_murmur_hash,
    pk_number_hash,
    pk_default_hash,
};

typedef std::vector<SortPattern> SortPatternVec;

const optionflag_t OPTION_FLAG_ALL = of_doc_payload | of_term_payload |
                                     of_position_payload | of_position_list |
                                     of_term_frequency;
const optionflag_t EXPACK_OPTION_FLAG_ALL = OPTION_FLAG_ALL | of_fieldmap;
const optionflag_t OPTION_FLAG_NONE = of_none;
const optionflag_t NO_PAYLOAD = of_position_list | of_term_frequency;
const optionflag_t NO_POSITION_LIST = of_doc_payload | of_term_payload | of_term_frequency;
const optionflag_t NO_TERM_FREQUENCY = of_doc_payload | of_term_payload;
const optionflag_t EXPACK_NO_PAYLOAD = of_position_list | of_term_frequency | of_fieldmap;
const optionflag_t EXPACK_NO_POSITION_LIST = of_doc_payload | of_term_payload | of_term_frequency | of_fieldmap;
const optionflag_t EXPACK_NO_TERM_FREQUENCY = of_doc_payload | of_term_payload | of_fieldmap;

const uint8_t SKIP_LIST_BUFFER_SIZE = 32;

const size_t DEFAULT_CHUNK_SIZE = 10; //10 MB

const uint64_t HASHMAP_EMPTY_KEY = (uint64_t)-1;
const uint64_t HASHMAP_EMPTY_VALUE = (uint64_t)-1;
const size_t HASHMAP_INIT_SIZE = 1024;

#define INDEX_FORMAT_VERSION "2.1.2"
#define DOC_PAYLOAD_FIELD_NAME "DOC_PAYLOAD"
#define BITMAP_TRUNCATE_INDEX_NAME "BITMAP"
#define MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME "main_docid_to_sub_docid_attr_name"
#define SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME "sub_docid_to_main_docid_attr_name"
#define INDEXLIB_REPORT_METRICS_INTERVAL (1 * 1000 * 1000); // 1s

#define INDEXLIB_BUILD_INFO_COLLECTOR_NAME "index_build_info"

static const std::string HA_RESERVED_TIMESTAMP =  "ha_reserved_timestamp";
static const std::string DOC_TIME_TO_LIVE_IN_SECONDS = "doc_time_to_live_in_seconds";

//TODO: fix the warning of "var unused" in indexlib.h
static inline void __FOR_UNUSED_VAR_FUNC__()
{
    (void)MAX_DOCID;
    (void)INVALID_POSITION;
    (void)MAX_FREE_MEMORY;
}

static constexpr uint32_t  DOCUMENT_BINARY_VERSION = 7;

namespace heavenask { namespace indexlib {
enum TableType
{
    tt_index,
    tt_kv,
    tt_kkv,
    tt_rawfile,
    tt_linedata,
    tt_customized,
};

enum IndexPreferenceType
{
    ipt_perf,
    ipt_store,
    ipt_unknown,
};

enum HashFunctionType
{
    hft_int64,
    hft_uint64,
    hft_murmur,
    hft_unknown,
};

enum LevelTopology
{
    topo_default = 0x00,
    topo_sequence = 0x01,
    topo_hash_mod = 0x02,
    topo_hash_range = 0x04,
};

enum TableSearchCacheType
{
    tsc_default,    // search cache when enable cache;
    tsc_no_cache,   // not search cache
    tsc_only_cache, // search result only in cache
};

enum IndexStatus
{
    is_normal = 0x01,
    is_disable = 0x02,
    is_deleted = 0x04,
};

enum ConfigIteratorType
{
    CIT_NORMAL = is_normal,
    CIT_DISABLE = is_disable,
    CIT_DELETE = is_deleted,
    CIT_NOT_DELETE = is_normal | is_disable,
    CIT_NOT_NORMAL = is_disable | is_deleted,
    CIT_ALL = is_normal | is_deleted | is_disable,
};

const int64_t INVALID_TTL = -1;
const docid_t END_DOCID = std::numeric_limits<docid_t>::max();

struct PartitionRange {
    uint32_t from = 0;
    uint32_t to = 0;

    PartitionRange(uint32_t f, uint32_t t) : from(f), to(t) {}
    PartitionRange() = default;
    bool IsValid() { return from != to; }
    bool operator==(const PartitionRange &other) const
    {
      return from == other.from && to == other.to;
    }
};
}}

#endif
