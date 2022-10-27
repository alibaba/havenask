#include <iostream>
#include <sys/time.h>
#include <fstream>
#include <string>
#include <vector>
#include <unistd.h>
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"

using namespace std;
using namespace std::tr1;

IE_NAMESPACE_USE(common);

struct Options
{
    Options() : testMode(0), compType(2), list(0) {}

    string dataFile;
    int testMode; // 1 for testing compress and decompress performance, 2 for testing decompress perf, 3 for checking correctness
    int compType; // 1 for new pfor-delta, 2 for pfor-delta
    int list; // 0 for all list, 1 for doc list, 2 for tf list, 3 for pos list
};

class CompressorWrapper
{
public:
    static size_t NewPFDCompress(uint32_t* dest, size_t destLen, const uint32_t* src, size_t srcLen)
    {
        return mNewPFDCompressor.CompressInt32(dest, destLen, src, srcLen);
    }

    static size_t PFDCompress(uint32_t* dest, size_t destLen, const uint32_t* src, size_t srcLen)
    {
        ssize_t ret = PforDeltaCompressor::Compress32((uint8_t*)dest, (destLen << 2), src, srcLen);
        if (ret <= 0)
        {
            return 0;
        }
        return ((size_t)ret >> 2);
    }

    static size_t NewPFDDecompress(uint32_t* dest, size_t destLen, const uint32_t* src, size_t srcLen)
    {
        return mNewPFDCompressor.DecompressInt32(dest, destLen, src, srcLen);
    }

    static size_t PFDDecompress(uint32_t* dest, size_t destLen, const uint32_t* src, size_t srcLen)
    {
        ssize_t ret = PforDeltaCompressor::Decompress32(dest, destLen, (const uint8_t*)src, (srcLen << 2));
        if (ret <= 0)
        {
            return 0;
        }
        return ((size_t)ret);
    }

private:
    static NewPForDeltaCompressor mNewPFDCompressor;
};

NewPForDeltaCompressor CompressorWrapper::mNewPFDCompressor;

void PrintUsage(int argc, char *argv[])
{
    string appName = argv[0];
    size_t pos = appName.find_last_of('/');
    if (pos != string::npos)
    {
        appName = appName.substr(pos + 1);
    }

    cout << "Usage: \n"  
         << "Test compression and decompression performance: \n\t"
         << appName << " -p org_index_data -t pfd|newpfd -l doc|tf|pos" << endl
         << "Check the correctness of compression/decompression: \n\t"
         << appName << " -c org_index_data -t pfd|newpfd -l doc|tf|pos" << endl
         << "Test decompression: \n\t"
         << appName << " -d compressed_data -t pfd|newpfd" << endl;
    cout << endl;
}

bool CheckOption(Options &opt)
{
    if (opt.testMode != 1 && opt.testMode != 2 && opt.testMode != 3)
    {
        return false;
    }

    if (opt.dataFile.empty())
    {
        cout << "No data file specified." << endl;
        return false;
    }

    if (opt.list < 0 || opt.list > 3)
    {
        cout << "Invalid list type: must be \'doc\' or \'tf\' or \'pos\'." << endl;
        return false;
    }

    if (opt.compType != 1 && opt.compType != 2)
    {
        cout << "Invalid compression type: must be \'newpfd\' or \'pfd\'" << endl;
        return false;
    }

    return true;
}

bool ParseOption(int argc, char *argv[], Options &opt) 
{
    const char * const short_options = "d:p:t:h:c:l:";
    int option;

    while( (option = getopt(argc, argv, short_options)) != -1 )
    {
        switch(option)
        {
        case 'p':
            opt.testMode = 1;
            opt.dataFile = optarg;
            break;
        case 'd':
            opt.testMode = 2;
            opt.dataFile = optarg;
            break;

        case 'c':
            opt.testMode = 3;
            opt.dataFile = optarg;
            break;

        case 'l':
            if (!strcmp(optarg, "doc"))
            {
                opt.list = 1;
            }
            else if (!strcmp(optarg, "tf"))
            {
                opt.list = 2;
            }
            else if (!strcmp(optarg, "pos"))
            {
                opt.list = 3;
            }
            else 
            {
                opt.list = -1;
            }
            break;

        case 't':
            if (!strcmp(optarg, "newpfd"))
            {
                opt.compType = 1;
            }
            else if (!strcmp(optarg, "pfd"))
            {
                opt.compType = 2;
            }
            else
            {
                cout << "Unknown compression type" << optarg << endl;
                return false;
            }
            break;

        case 'h':
            PrintUsage(argc, argv);
            break;

        default:
            cout << "Unknown option." << optarg << endl;
            return false;
        }
    }
    if (!CheckOption(opt))
    {
        PrintUsage(argc, argv);
        return false;
    }
    return true;
}

#define ADD_TO_VECTOR(v, buf, s, minus)         \
    for (size_t i = 0; i < s; ++i)              \
    {                                           \
        v.push_back(buf[i] - minus);            \
    }

#define IF_AND_SKIP_DOC_OR_TF_LIST(l1, l2)      \
    if (l2 != 0 && l1 != l2)                    \
    {                                           \
        while (inStream.good())                 \
        {                                       \
            inStream.get(flag);                                         \
            if (flag == '4')                                            \
            {                                                           \
                inStream.read((char*)&length, sizeof(uint32_t));        \
                assert(length <= BLOCK_SIZE);                           \
                inStream.read((char*)readBuf, length * sizeof(docid_t)); \
            }                                                           \
            else                                                        \
            {                                                           \
                break;                                                  \
            }                                                           \
        }                                                               \
        break;                                                          \
    }

#define IF_AND_SKIP_POS_LIST(l1, l2)                   \
    if (l2 != 0 && l1 != l2)                    \
    {                                           \
        bool hasPayload;                        \
        inStream.read((char*)&hasPayload, sizeof(bool));        \
        while (inStream.good())                                 \
        {                                                       \
            inStream.get(flag);                                 \
            if (flag == '4')                                    \
            {                                                           \
               inStream.read((char*)&length, sizeof(uint32_t));         \
               assert(length <= BLOCK_SIZE);                            \
               if (!hasPayload)                                         \
               {                                                        \
                   inStream.read((char*)readBuf, length * sizeof(pos_t)); \
               }                                                        \
               else                                                     \
               {                                                        \
                   inStream.read((char*)readBuf, length * sizeof(pos_t)); \
                   inStream.read((char*)readBuf, length * sizeof(pospayload_t)); \
               }                                                        \
            }                                                           \
            else                                                        \
            {                                                           \
                assert(flag == '1' || flag == '0');                         \
                if (flag =='0' && buf.size() >= BUFFER_SIZE)            \
                {                                                       \
                inStream.unget();                                       \
                return totalLength;                                     \
                }                                                       \
                break;                                                  \
            }                                                           \
        }                                                               \
        break;                                                          \
    }

size_t ReadData(vector<uint32_t>& buf, ifstream& inStream, int listType)
{
    const static size_t BLOCK_SIZE = 128;
    const static size_t BUFFER_SIZE = 20 * 1024 * 1024;
    buf.reserve(BUFFER_SIZE);
    uint32_t readBuf[BLOCK_SIZE];

    size_t totalLength = 0;
    uint32_t length;
    char flag;
    inStream.get(flag);
    while (inStream.good())
    {
        switch(flag)
        {
        case '0': // a new term
        {
            inStream.read((char*)&length, sizeof(uint32_t));
            inStream.read((char*)readBuf, length);
            inStream.get(flag);
            break;
        }
        case '1': // docid list
        {
            IF_AND_SKIP_DOC_OR_TF_LIST(1, listType);
            buf.push_back(0);
            size_t lenPos = buf.size() - 1;
            while (inStream.good())
            {
                inStream.get(flag);
                if (flag == '4')
                {
                    inStream.read((char*)&length, sizeof(uint32_t));
                    assert(length <= BLOCK_SIZE);
                    totalLength += length;

                    inStream.read((char*)readBuf, length * sizeof(docid_t));
                    buf[lenPos] += length;
                    ADD_TO_VECTOR(buf, readBuf, length, 0);
                }
                else
                {
                    assert(flag == '2');
                    break;
                }
            }
            break;
        }
        case '2': // tf list
        {
            IF_AND_SKIP_DOC_OR_TF_LIST(2, listType);
            buf.push_back(0);
            size_t lenPos = buf.size() - 1;
            while (inStream.good())
            {
                inStream.get(flag);
                if (flag == '4')
                {
                    inStream.read((char*)&length, sizeof(uint32_t));
                    assert(length <= BLOCK_SIZE);
                    totalLength += length;

                    inStream.read((char*)readBuf, length * sizeof(docid_t));
                    buf[lenPos] += length;
                    ADD_TO_VECTOR(buf, readBuf, length, 1);
                }
                else
                {
                    assert(flag == '3');
                    break;
                }
            }
            break;
        }
        case '3': // pos list
        {
            IF_AND_SKIP_POS_LIST(3, listType);
            buf.push_back(0);
            size_t lenPos = buf.size() - 1;
            bool hasPayload;
            inStream.read((char*)&hasPayload, sizeof(bool));
            while (inStream.good())
            {
                inStream.get(flag);
                if (flag == '4')
                {
                    inStream.read((char*)&length, sizeof(uint32_t));
                    assert(length <= BLOCK_SIZE);
                    totalLength += length;

                    if (!hasPayload)
                    {
                        inStream.read((char*)readBuf, length * sizeof(pos_t));
                        buf[lenPos] += length;
                        ADD_TO_VECTOR(buf, readBuf, length, 0);
                    }
                    else
                    {
                        inStream.read((char*)readBuf, length * sizeof(pos_t));
                        buf[lenPos] += length;
                        ADD_TO_VECTOR(buf, readBuf, length, 0);

                        //skip pos payload
                        inStream.read((char*)readBuf, length * sizeof(pospayload_t));
                    }
                }
                else
                {
                    assert(flag == '1' || flag == '0');
                    if (flag =='0' && buf.size() >= BUFFER_SIZE)
                    {
                        inStream.unget();
                        return totalLength;
                    }
                    break;
                }
            }
            break;
        } //end case
        }// end switch
    }//end while
    return totalLength;
}

uint64_t GetCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t)tv.tv_sec * 1000000 + tv.tv_usec);
}

typedef size_t (CompressFunc)(uint32_t* dest, size_t destLen, const uint32_t* src, size_t srcLen);

size_t DoCompress(uint32_t* dest, size_t& destLen, const uint32_t* src, size_t srcLen, CompressFunc func)
{
    const uint32_t* srcPtr = src;
    uint32_t* destPtr = dest;

    size_t totalDestLen = 0;
    size_t leftLen = srcLen;
    size_t destLeftLen = destLen;

    while (leftLen > 0)
    {
        size_t curLen = *srcPtr++;
        assert(curLen != 0);

        uint32_t& curDestLen = *destPtr++;
        --destLeftLen;
        curDestLen = func(destPtr, destLeftLen, srcPtr, curLen);
        if (curDestLen == 0)
        {
            cerr << "Error occur when compressing!" << endl;
        }
        totalDestLen += curDestLen;
        srcPtr += curLen;
        leftLen -= (curLen + 1);

        destPtr += curDestLen;
        destLeftLen -= curDestLen;
    }
    destLen = destLeftLen;
    return totalDestLen;
}

typedef size_t (DecompressFunc)(uint32_t* dest, size_t destLen, const uint32_t* src, size_t srcLen);

size_t DoDecompress(uint32_t* dest, size_t& destLen, const uint32_t* src, size_t srcLen, DecompressFunc func)
{
    const uint32_t* srcPtr = src;
    uint32_t* destPtr = dest;

    size_t totalDestLen = 0;
    size_t leftLen = srcLen;
    size_t destLeftLen = destLen;
    while (leftLen > 0)
    {
        size_t curLen = *srcPtr++;
        assert(curLen != 0);

        uint32_t& curDestLen = *destPtr++;
        --destLeftLen;
        curDestLen = func(destPtr, destLeftLen, srcPtr, curLen);
        if (curDestLen == 0)
        {
            cerr << "Error occur when compressing!" << endl;
        }
        totalDestLen += curDestLen;
        srcPtr += curLen;
        leftLen -= (curLen + 1);

        destPtr += curDestLen;
        destLeftLen -= curDestLen;
    }
    destLen = destLeftLen;
    return totalDestLen;
}

void TestCompressAndDecompressPerf(const string& dataFile, const Options& opt, bool check)
{
    ifstream inStream;
    inStream.open(dataFile.c_str(), ifstream::in);

    size_t testCount = 0;

    uint64_t totalCompTime = 0;
    uint64_t totalDecompTime = 0;
    size_t totalDataLen = 0;
    size_t totalEncodedLen = 0;
    size_t totalDecodedLen = 0;
    bool checkAndOk = true;
    while (inStream.good())
    {
        vector<uint32_t> buf;
        vector<uint32_t> dest;
        vector<uint32_t> encoded;

        size_t length = ReadData(buf, inStream, opt.list);
        totalDataLen += length;

        encoded.resize(buf.size());
        size_t encodedBufLen = encoded.size();

        //test compression
        {
            uint64_t startTime = GetCurrentTime();
            size_t encodedLen = 0;
            if (opt.compType == 1)
            {
                encodedLen = DoCompress((uint32_t*)&(encoded[0]), encodedBufLen, 
                        (const uint32_t*)&(buf[0]), buf.size(), CompressorWrapper::NewPFDCompress);
            }
            else 
            {
                encodedLen = DoCompress((uint32_t*)&(encoded[0]), encodedBufLen, 
                        (const uint32_t*)&(buf[0]), buf.size(), CompressorWrapper::PFDCompress);
            }

            totalEncodedLen += encodedLen;

            uint64_t endTime = GetCurrentTime();
            totalCompTime += (endTime - startTime);
        }

        size_t decodedLen = 0;
        {
            dest.resize(buf.size());
            size_t destLen = dest.size();        
            
            uint64_t startTime = GetCurrentTime();
            if (opt.compType == 1)
            {
                decodedLen = DoDecompress((uint32_t*)&(dest[0]), destLen, (const uint32_t*)&(encoded[0]), 
                        encoded.size() - encodedBufLen, CompressorWrapper::NewPFDDecompress);
            }
            else 
            {
                decodedLen = DoDecompress((uint32_t*)&(dest[0]), destLen, (const uint32_t*)&(encoded[0]), 
                        encoded.size() - encodedBufLen, CompressorWrapper::PFDDecompress);
            }

            totalDecodedLen += decodedLen;

            uint64_t endTime = GetCurrentTime();
            totalDecompTime += (endTime - startTime);
        }

        if (check)
        {
            if (decodedLen != length)
            {
                assert(false);
                cout << "ERROR: data length: " << length << " != " << "decoded length: " << decodedLen << endl;
                checkAndOk = false;
            }
            else 
            {
                for (size_t i = 0; i < (size_t)length; i++)
                {
                    if (buf[i] != dest[i])
                    {
                        assert(false);
                        cout << "ERROR: idx: " << i << " raw data: " << buf[i] << " != " << "decoded data: " << dest[i] << endl;
                        checkAndOk = false;
                    }
                }
            }
        }

        testCount++;
        if (testCount % 10 == 0)
        {
            cout << "Decoded: " << totalDataLen << endl;
        }

        if (testCount == 20)
        {
//            break;
        }
    }
    
    //in ms
    float timeElapseOfComp = ((float)totalCompTime) / 1000;

    if (check)
    {
        cout << "=================================" << endl;            

        if (checkAndOk)
        {
            cout << "Check compression correctness: OK. "<< endl;
        }
        else
        {
            cout << "Check compression correctness: ERROR. "<< endl;
        }
    }

    cout << "=================================" << endl;
    cout << "Total data size: " << (totalDataLen * 4) << " bytes" << endl;
    cout << "Total compressed size: " << (totalEncodedLen * 4) << " bytes" << endl;
    cout << "Compress ratio: " << ((float) totalEncodedLen/ (float)(totalDataLen) ) << endl;
    cout << "Total compress time: " << totalCompTime << " ms" << endl;
    cout << "Compress speed: " << (totalDataLen * 4 / (1024 * 1024)) / (timeElapseOfComp / 1000) 
         << " MB/s" << endl;

    float timeElapseOfDecomp = ((float)totalDecompTime) / 1000;
    cout << "Total decompress time: " << totalDecompTime << " ms" << endl;
    cout << "Decompress speed: " << (totalDataLen * 4 / (1024 * 1024)) / (timeElapseOfDecomp / 1000) 
         << " MB/s" << endl;
}


void TestDecompressPerf(const string& dataFile)
{
}

int main(int argc, char** argv) 
{
    Options opt;
    if (!ParseOption(argc, argv, opt)) 
    {
        return 0;
    }

    if (opt.testMode == 1) 
    {
        TestCompressAndDecompressPerf(opt.dataFile, opt, false);
    }
    else if (opt.testMode == 2)
    {
        TestDecompressPerf(opt.dataFile);
    }
    if (opt.testMode == 3) 
    {
        TestCompressAndDecompressPerf(opt.dataFile, opt, true);
    }
    return 0;
}


