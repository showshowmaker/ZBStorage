#define _GNU_SOURCE
#include <unordered_set>
#include <queue>
#include <list>
#include <array>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <vector>
#include <map>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fstream>
#include <algorithm>
#include <thread>
#include <future>
#include <unistd.h>
#include "masstree_wrapper.h" 
#include <filesystem>
#include <cstring>
#include <fcntl.h>
#define BIG_CONSTANT(x) (x##LLU)
using namespace std;
namespace fs = filesystem;
#define pagesize 4
#define keysize 20
#define valuesize 236
#define leafnum pagesize*1024/(keysize+valuesize)
// 定义文件信息结构体
struct FileInfo {
    uint32_t id;
    uint16_t nameLength;
    char type;
    uint64_t size;       // 文件大小
    time_t createTime;   // 文件创建时间
    time_t lastModifiedTime;  // 文件最近一次修改时间
    time_t lastAccessTime;    // 文件最近一次访问时间
};

// 全局变量
static uint64_t numEntries = 0; // 记录已分配叶子节点个数
string logPath = "/mnt/NVMe/log";//"../log" //"/mnt/NVMe/log"

// 生成随机数据填充数组
void fillRandomData(char* data, int size) {
    // 定义随机字符集，这里使用可打印 ASCII 字符集
    const char printableChars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+[]{};:'\",.<>?/\\|`~";

    // 计算随机字符集的长度
    const int printableCharsLength = sizeof(printableChars) - 1;

    // 填充数组
    for (int i = 0; i < size; ++i) {
        // 从随机字符集中随机选择一个字符填充数组
        data[i] = printableChars[rand() % printableCharsLength];
    }
}
uint64_t MurmurHash64A ( const void * key, int len)
{
    uint64_t seed=0;
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);

    while(data != end)
    {
        uint64_t k = *data++;

        k *= m; 
        k ^= k >> r; 
        k *= m; 
        
        h ^= k;
        h *= m; 
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(len & 7)
    {
    case 7: h ^= ((uint64_t) data2[6]) << 48;
    case 6: h ^= ((uint64_t) data2[5]) << 40;
    case 5: h ^= ((uint64_t) data2[4]) << 32;
    case 4: h ^= ((uint64_t) data2[3]) << 24;
    case 3: h ^= ((uint64_t) data2[2]) << 16;
    case 2: h ^= ((uint64_t) data2[1]) << 8;
    case 1: h ^= ((uint64_t) data2[0]);
            h *= m;
    };
    
    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
} 

string generateID(const string& filepath, uint32_t uid) {
    // 获取 filepath 中的父目录路径和文件名
    size_t pos = filepath.find_last_of('/');
    string parent_dir = filepath.substr(0, pos); // 父目录路径
    string filename = filepath.substr(pos + 1); // 文件名

    // 对父目录路径和文件名分别进行 MurmurHash2 算法散列
    uint64_t hash_parent_dir = MurmurHash64A(parent_dir.c_str(), parent_dir.length());
    uint64_t hash_filename = MurmurHash64A(filename.c_str(), filename.length());
    // 创建缓冲区来保存拼接后的字节流
    string result(keysize, '\0');

    // 使用 memcpy 将数据复制到缓冲区中
    char* ptr = &result[0];
    memcpy(ptr, &uid, sizeof(uid));
    ptr += sizeof(uid);
    memcpy(ptr, &hash_parent_dir, sizeof(hash_parent_dir));
    ptr += sizeof(hash_parent_dir);
    memcpy(ptr, &hash_filename, sizeof(hash_filename));

    return result;
}
// 分配磁盘空间函数
uint64_t allocateDiskSpace(char* buffer,int size) {
    // 计算当前文件索引和偏移量
    uint64_t currentFileIndex = numEntries / (128 * 1024 / pagesize);
    int offset = numEntries % (128 * 1024 / pagesize) * pagesize*1024;
    // 构造文件名
    string fileName = logPath + "/log_" + to_string(currentFileIndex) + ".txt";
    // 打开文件
    int fd = open(fileName.c_str(), O_WRONLY | O_CREAT , S_IRUSR | S_IWUSR);
    if (fd == -1) {
        cerr << "Error opening file: " << fileName << endl;
        return -1;
    }

    // 移动文件指针到指定位置
    if (lseek(fd, offset, SEEK_SET) == -1) {
        //cin>>kkkk;
        cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
        close(fd);
        return -1;
    }
    // 使用pwrite()直接写入文件内容
    ssize_t bytesWritten = write(fd, buffer, (keysize+valuesize)*size);
    close(fd);
    if (bytesWritten == -1) {
        cerr << "Error writing file: " << fileName << endl;
        return -1;
    }

    // 返回地址并增加叶子节点计数
    return numEntries++;
}
// 读取磁盘操作函数
void readDisk(uint64_t found_address,char* buffer) {
    // 计算文件索引和偏移量
    uint64_t fileIndex = found_address / (128 * 1024 / pagesize);
    int alignedOffset = found_address % (128 * 1024 / pagesize) * pagesize*1024;
    // 构造文件名
    string fileName = logPath + "/log_" + to_string(fileIndex) + ".txt";
    // 打开文件
    int fd = open(fileName.c_str(), O_RDONLY | O_DIRECT);
    if (fd == -1) {
        cerr << "Error opening file: " << fileName << " - " << strerror(errno) << endl;
        return;
    }
    if (!buffer) {
        cerr << "Error allocating memory for buffer." << endl;
        close(fd);
        return;
    }
    // 使用pread()直接读取文件内容到缓冲区
    ssize_t bytesRead = pread(fd, buffer, pagesize*1024, alignedOffset);
    if (bytesRead == -1) {
        cerr << "Error reading file: " << fileName << " - " << strerror(errno) << endl;
        close(fd);
        free(buffer);
        return;
    }
    // 关闭文件并释放缓冲区
    close(fd);
}

void readfromLeafNode(MasstreeWrapper& masstree_wrapper, const string& key) {
    // 创建一个向量来存储扫描结果
    vector<pair<string,uint64_t>> result;
    masstree_wrapper.scan(key, 1, result);
    if (result.empty()) {
        cout << "Leaf node not found for key: " << key << endl;
    } else {
        uint64_t combinedData = result[0].second;
        uint64_t oldnum = combinedData>>8;
        int size=combinedData&0xFF;
        char *buffer = (char*)aligned_alloc(4096, 1024*pagesize);
        // 从磁盘中读取叶子节点的数据到 buffer 中
        readDisk(oldnum, buffer);
        // 假设数据格式为键值对依次存储
        char* ptr = buffer;
        int low=0;
        int high=size-1;
        int mid;
        const char* keyCStr=key.c_str();
        while (low <= high) {
                mid = low + (high - low) / 2;
                int compareResult = memcmp(buffer + mid * (keysize+valuesize), keyCStr, keysize);
                if (compareResult == 0) {
                    // 找到了
                    

                    // 释放内存
                    free(buffer);
                    return;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        // 释放内存
        free(buffer);
        cout << "Value not found for key: " << key << endl;
        // if (it != leafData.end()) {
        //     const char* buffer = it->second.c_str();
        //     //cout << "Value found for key " << key <<":"<<buffer[0]<<buffer[1]<< endl;
        //     // 恢复相关字段
        //     // char type = buffer[0];
        //     // uint64_t size;
        //     // memcpy(&size, buffer + 1, sizeof(uint64_t));
        //     // time_t createTime;
        //     // memcpy(&createTime, buffer + 1 + sizeof(uint64_t), sizeof(time_t));
        //     // time_t lastModifiedTime;
        //     // memcpy(&lastModifiedTime, buffer + 1 + sizeof(uint64_t) + sizeof(time_t), sizeof(time_t));
        //     // time_t lastAccessTime;
        //     // memcpy(&lastAccessTime, buffer + 1 + sizeof(uint64_t) + 2 * sizeof(time_t), sizeof(time_t));
        //     // char storageLocation[5];
        //     // memcpy(storageLocation, buffer + 1 + sizeof(uint64_t) + 3 * sizeof(time_t), sizeof(storageLocation));
        //     // char otherProperties[218];
        //     // memcpy(otherProperties, buffer + 1 + sizeof(uint64_t) + 3 * sizeof(time_t) + sizeof(storageLocation), sizeof(otherProperties));

        //     //输出找到的值
        //     // cout << "Type: " << type << endl;
        //     // cout << "Size: " << size << endl;
        //     // cout << "Create Time: " << ctime(&createTime);
        //     // cout << "Last Modified Time: " << ctime(&lastModifiedTime);
        //     // cout << "Last Access Time: " << ctime(&lastAccessTime);
        //     // cout << "Storage Location: " << string(storageLocation, sizeof(storageLocation)) << endl;
        //     // cout << "Other Properties: " << string(otherProperties, sizeof(otherProperties)) << endl;

        // } else {
        //     cout << "Value not found for key: " << key << endl;
        // }
    }
}

void readAndUpdateLeafNode(MasstreeWrapper& masstree_wrapper, const string& key, char* value) {
    // 创建一个向量来存储扫描结果
    vector<pair<string,uint64_t>> result;
    masstree_wrapper.scan(key,1,result);
    if(result.empty())
    {
        char kv[keysize+valuesize];
        // 将键值对编码到 kv 中
        memcpy(kv, key.c_str(), keysize);
        memcpy(kv + keysize, value, valuesize);
        masstree_wrapper.rscan(key,1,result);
        if(result.empty()||((result[0].second)&0xFF)==leafnum)
        {
            //cout<<"新建"<<endl;
            uint64_t newnum=allocateDiskSpace(kv,1);
            uint8_t leafNodeCount = 1; // 新建的叶子节点个数为 1
            uint64_t combinedData = (newnum << 8) | leafNodeCount;
            masstree_wrapper.insert(key,combinedData);
        }else
        {
            //cout<<"插入小的"<<endl;
            uint64_t combinedData = result[0].second;
            uint64_t oldnum = combinedData>>8;
            uint8_t size=combinedData&0xFF;
            // 计算文件索引和偏移量
            uint64_t fileIndex = oldnum / (128 * 1024 / pagesize);
            int alignedOffset = oldnum % (128 * 1024 / pagesize) * 1024*pagesize;
            // 构造文件名
            string fileName = logPath + "/log_" + to_string(fileIndex) + ".txt";
            // 打开文件
            int fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            if (fd == -1) {
                cerr << "Error opening file: " << fileName << " - " << strerror(errno) << endl;
                return;
            }
            if (lseek(fd, alignedOffset+(keysize+valuesize)*size, SEEK_SET) == -1) {
                cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                close(fd);
                //cin>>kkkk;
                return;
            }
            ssize_t bytesWritten = write(fd, kv, keysize+valuesize);
            close(fd);
            // 检查写入是否成功
            if (bytesWritten == -1) {
                cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                return;
            }
            uint8_t leafNodeCount = size+1; 
            //cout<<"oldnum::::"<<oldnum<<endl;
            combinedData = (oldnum << 8) | leafNodeCount;
            masstree_wrapper.remove(result[0].first);
            masstree_wrapper.insert(key,combinedData);
            return;
        }

        // cout<<"key:   "<<key<<endl;
        // cout<<"address:   "<<(combinedData>>8)<<endl;
        // cout<<"size:   "<<(combinedData&0xFF)<<endl;
    }else{  
        //string nowkey=result[0].first;
        // cout<<"key:"<<key<<endl;
        // cout<<"nowkey:"<<nowkey<<endl;
        uint64_t combinedData = result[0].second;
        uint64_t oldnum = combinedData>>8;
        int size=combinedData&0xFF;
        // 计算文件索引和偏移量
        uint64_t fileIndex = oldnum / (128 * 1024 / pagesize);
        int alignedOffset = oldnum % (128 * 1024 / pagesize) * pagesize*1024;
        // 构造文件名
        string fileName = logPath + "/log_" + to_string(fileIndex) + ".txt";
        // 打开文件
        int fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            cerr << "Error opening file: " << fileName << " - " << strerror(errno) << endl;
            return;
        }
        // 移动文件指针到指定位置
        if (lseek(fd, alignedOffset, SEEK_SET) == -1) {

            cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
            close(fd);
            return;
        }
        char buffer[(keysize+valuesize)*(size+2)];
        // 使用普通读取方式读取文件内容到缓冲区
        ssize_t bytesRead = read(fd, buffer+(keysize+valuesize), (keysize+valuesize)*size);
        //cout<<"bytesRead:"<<bytesRead <<endl;
        //int kkkk;
        if (bytesRead == -1) {
            cerr << "Error reading file: " << fileName << " - " << strerror(errno) << endl;
            close(fd);
            //cin>>kkkk;
            return;
        }
        // 
        if(size==leafnum)
        {
            //cout<<"分裂"<<endl;
            char* ptr;
            const char* keyCStr = key.c_str(); 
            int low=1;
            int high=size;
            int mid;
            while (low <= high) {
                mid = low + (high - low) / 2;
                int compareResult = memcmp(buffer + mid * (keysize+valuesize), keyCStr, keysize);
                if (compareResult == 0) {
                    // Handle case where the key already exists
                    char *ptr=buffer+(keysize+valuesize)*mid+keysize;
                    memcpy(ptr, value, valuesize);
                    if (lseek(fd, alignedOffset+(keysize+valuesize)*(mid-1), SEEK_SET) == -1) {
                        cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                        close(fd);
                        //cin>>kkkk;
                        return;
                    }
                    ssize_t bytesWritten = write(fd, ptr, valuesize);
                    close(fd);
                    // 检查写入是否成功
                    if (bytesWritten == -1) {
                        cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                        return;
                    }
                    return;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }//low-1个小于  size-low+1个大于
            if(low<=leafnum/2)
            {
                if (lseek(fd, alignedOffset, SEEK_SET) == -1) {
                    cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                    close(fd);
                    //cin>>kkkk;
                    return;
                }
                ssize_t bytesWritten = write(fd, buffer + (leafnum/2+1) * (keysize+valuesize), (leafnum-leafnum/2)*(keysize+valuesize));
                close(fd);
                // 检查写入是否成功
                if (bytesWritten == -1) {
                    cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                    return;
                }
                uint8_t leafNodeCount = leafnum-leafnum/2; 
                uint64_t combinedData = (oldnum << 8) | leafNodeCount;
                masstree_wrapper.insert(result[0].first,combinedData);
                ptr=buffer+(keysize+valuesize)*low;
                memmove(ptr+(keysize+valuesize),ptr,(leafnum/2+1-low)*(keysize+valuesize));
                memcpy(ptr, keyCStr, keysize);
                ptr+=keysize;
                memcpy(ptr, value, valuesize);
                uint64_t newnum=allocateDiskSpace(buffer+(keysize+valuesize),leafnum/2+1);
                // 构造叶子节点数据
                leafNodeCount = leafnum/2+1; 
                combinedData = (newnum << 8) | leafNodeCount;
                masstree_wrapper.insert(string(buffer + (leafnum/2+1) * (keysize+valuesize),keysize),combinedData);
                return;
            }else
            {
                uint64_t newnum=allocateDiskSpace(buffer+(keysize+valuesize),leafnum/2);
                // 构造叶子节点数据
                uint8_t leafNodeCount = leafnum/2; // 新建的叶子节点个数为(ptr-buffer)/256
                uint64_t combinedData = (newnum << 8) | leafNodeCount;
                masstree_wrapper.insert(string(buffer + (leafnum/2) * (keysize+valuesize),keysize),combinedData);
                memmove(buffer+(leafnum/2)*(keysize+valuesize),buffer+(leafnum/2+1)*(keysize+valuesize),(low-(leafnum/2+1))*(keysize+valuesize));
                ptr=buffer+(keysize+valuesize)*(low-1);
                memcpy(ptr, keyCStr, keysize);
                ptr+=keysize;
                memcpy(ptr, value, valuesize);
                if (lseek(fd, alignedOffset, SEEK_SET) == -1) {
                    cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                    close(fd);
                    //cin>>kkkk;
                    return;
                }
                ssize_t bytesWritten = write(fd, buffer + leafnum/2 * (keysize+valuesize), (leafnum-leafnum/2+1)*(keysize+valuesize));
                close(fd);
                // 检查写入是否成功
                if (bytesWritten == -1) {
                    cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                    return;
                }
                leafNodeCount = leafnum-leafnum/2+1; 
                combinedData = (oldnum << 8) | leafNodeCount;
                masstree_wrapper.insert(result[0].first,combinedData);
                return;
            }
            // ptr=buffer+(keysize+valuesize)*low;
            // if (lseek(fd, alignedOffset, SEEK_SET) == -1) {
            //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
            //     close(fd);
            //     //cin>>kkkk;
            //     return;
            // }
            // ssize_t bytesWritten = write(fd, ptr, (leafnum+1-low)*(keysize+valuesize));
            // close(fd);
            // // 检查写入是否成功
            // if (bytesWritten == -1) {
            //     cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
            //     return;
            // }
            // uint8_t leafNodeCount = leafnum+1-low; 
            // uint64_t combinedData = (oldnum << 8) | leafNodeCount;
            // masstree_wrapper.insert(result[0].first,combinedData);
            // memcpy(ptr, key.c_str(), keysize);
            // ptr+=keysize;
            // memcpy(ptr, value, valuesize);
            // uint64_t newnum=allocateDiskSpace(buffer+(keysize+valuesize),low);
            // // 构造叶子节点数据
            // leafNodeCount = low; // 新建的叶子节点个数为(ptr-buffer)/256
            // //cout<<"新的:"<<leafNodeCount<<endl;
            // combinedData = (newnum << 8) | leafNodeCount;
            // masstree_wrapper.insert(key,combinedData);
            // return;
        }
        else
        {
            //cout<<"插入旧的"<<endl;
            int low=1;
            int high=size;
            int mid;
            const char* keyCStr = key.c_str(); 
            while (low <= high) {
                mid = low + (high - low) / 2;
                int compareResult = memcmp(buffer + mid * (keysize+valuesize), keyCStr, keysize);
                if (compareResult == 0) {
                    // Handle case where the key already exists
                    char *ptr=buffer+(keysize+valuesize)*mid+keysize;
                    memcpy(ptr, value, valuesize);
                    if (lseek(fd, alignedOffset+(keysize+valuesize)*(mid-1), SEEK_SET) == -1) {
                        cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                        close(fd);
                        //cin>>kkkk;
                        return;
                    }
                    ssize_t bytesWritten = write(fd, ptr, valuesize);
                    close(fd);
                    // 检查写入是否成功
                    if (bytesWritten == -1) {
                        cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                        return;
                    }
                    return;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }//low-1个小于  size-low+1个大于
            char *ptr=buffer+(keysize+valuesize)*low;
            ptr-=valuesize;
            memcpy(ptr, value, valuesize);
            ptr-=keysize;
            memcpy(ptr, key.c_str(), keysize);
            if (lseek(fd, alignedOffset+(keysize+valuesize)*(low-1), SEEK_SET) == -1) {
                cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                close(fd);
                //cin>>kkkk;
                return;
            }
            ssize_t bytesWritten = write(fd, ptr, (size-low+2)*(keysize+valuesize));//size-low+2
            close(fd);
            // 检查写入是否成功
            if (bytesWritten == -1) {
                cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                return;
            }
            uint8_t leafNodeCount = size+1; // 新建的叶子节点个数为 1
            uint64_t combinedData = (oldnum << 8) | leafNodeCount;
            masstree_wrapper.insert(result[0].first,combinedData);
            return;
        }
    }
}
void readFileInfoFromMap(const string& filePath, MasstreeWrapper& mt_wrapper, uint32_t uid)
{
    // 打开保存文件信息的文本文件
    ifstream inputFile(filePath);
    if (!inputFile) {
        cerr << "Error opening input file: " << filePath << endl;
        return;
    }

    string encryptedPath;
    char type;
    uint16_t nameLength;
    uint64_t size;
    time_t createTime;
    time_t lastModifiedTime;
    time_t lastAccessTime;
    uint64_t address;
    char storageLocation[5];
    char otherProperties[valuesize-38];
    char value[valuesize];
    int offset;
    // 将各个字段逐个拷贝到value中
    int num = 1000000;
    while (getline(inputFile, encryptedPath)&&num--) { //读单独拿出来 
        inputFile >> nameLength >> type >> size >> createTime >> lastModifiedTime >> lastAccessTime;
        inputFile.ignore(); // 忽略换行符
        // 扩展路径
        // 创建查询键
        string key = generateID(encryptedPath,uid);
        // 填充随机数据
        fillRandomData(storageLocation, sizeof(storageLocation));
        fillRandomData(otherProperties, sizeof(otherProperties));
        // 将各个字段逐个拷贝到value中
        offset = 0;
        memcpy(value + offset, &type, sizeof(char));
        offset += sizeof(char);
        memcpy(value + offset, &size, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        memcpy(value + offset, &createTime, sizeof(time_t));
        offset += sizeof(time_t);
        memcpy(value + offset, &lastModifiedTime, sizeof(time_t));
        offset += sizeof(time_t);
        memcpy(value + offset, &lastAccessTime, sizeof(time_t));
        offset += sizeof(time_t);
        memcpy(value + offset, storageLocation, sizeof(storageLocation));
        offset += sizeof(storageLocation);
        memcpy(value + offset, otherProperties, sizeof(otherProperties));
        readAndUpdateLeafNode(mt_wrapper, key, value);
    }
    inputFile.close();
}
void deleteAllFilesInDirectory(const string& directory) {
    try {
        for (const auto& entry : fs::directory_iterator(directory)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
        cout << "All files in directory " << directory << " have been deleted." << endl;
    } catch (const fs::filesystem_error& e) {
        cerr << "Error deleting files: " << e.what() << endl;
    }
}
// 获取进程的物理内存占用情况
void printMemoryUsage() {
    ifstream file("/proc/self/status");
    string line;
    while (getline(file, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            cout << "Memory usage: " << line.substr(7) << endl;
            break;
        }
    }
    file.close();
 }
// int main(){
//     deleteAllFilesInDirectory(logPath); 
//     MasstreeWrapper mt_wrapper;

//     for(int i=49;i>=10;i--)
//     {
//         if(i==48) continue;
//         cout<<i<<":::::::::"<<endl;
//         char value[236];
//         string tmp=to_string(i);
//         value[0]=tmp[0];
//         value[1]=tmp[1];
//         readAndUpdateLeafNode(mt_wrapper,"123456789123456789"+tmp,value);
// cout<<100-i<<":::::::::"<<endl;
//                 tmp=to_string(100-i);
//         value[0]=tmp[0];
//         value[1]=tmp[1];
//         readAndUpdateLeafNode(mt_wrapper,"123456789123456789"+tmp,value);
//     }
//     int i=48;
//             cout<<i<<":::::::::"<<endl;
//         char value[236];
//         string tmp=to_string(i);
//         value[0]=tmp[0];
//         value[1]=tmp[1];
//         readAndUpdateLeafNode(mt_wrapper,"123456789123456789"+tmp,value);




//     for(int i=1;i<=99;i++)
//     {
//         char value[236];

//         string tmp=to_string(i);

//         readfromLeafNode(mt_wrapper,"123456789123456789"+tmp);
//     }





//     return 0;
// }

/*   并行   */
// typedef struct {
//     MasstreeWrapper& masstree_wrapper;
//     vector<string>& key_list;
//     int start_index;
//     int end_index;
// } ThreadData;

// void search_keys(void* arg) {
//     ThreadData* data = (ThreadData*)arg;
//     for (int i = data->start_index; i < data->end_index; ++i) {
//         readfromLeafNode(data->masstree_wrapper, data->key_list[i]);
//     }
//     pthread_exit(NULL); // 退出线程
// }

// void parallel_search(MasstreeWrapper& masstree_wrapper, vector<string>& key_list, int num_threads) {//需要在主函数对应调用前增加对于key_list的初始化
//     pthread_t threads[num_threads]; //创建线程数组
//     ThreadData thread_data[num_threads];
//     int num_keys = key_list.size();
//     int keys_per_thread = num_keys / num_threads;
//     for (int i = 0; i < num_threads; ++i) {
//         thread_data[i].masstree_wrapper = masstree_wrapper;
//         thread_data[i].key_list = key_list;
//         thread_data[i].start_index = i * keys_per_thread;
//         thread_data[i].end_index = (i == num_threads - 1) ? num_keys : (i + 1) * keys_per_thread;
//         pthread_create(&threads[i], NULL, search_keys, (void*)&thread_data[i]);
//     }
//     for (int i = 0; i < num_threads; ++i) {
//         pthread_join(threads[i], NULL);
//     }
// }
/*   ......   */

int main() {
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    MasstreeWrapper mt_wrapper;
    int choose;
    vector<string> allFilePaths;
    string encryptedPath;
    int n;
    int low;
    int high;
    int k;
    pid_t pid;
    int tasksPerThread;
    int numThreads;
    int numTasks;
    ifstream inputFile;
    const string filePath =  "../txt/filename.txt";
    string infoFilePath;
    deleteAllFilesInDirectory(logPath); 
    allFilePaths.clear();
    vector<std::thread> threads;
    // 打开保存文件信息的文本文件,生成随机路径
    inputFile.open(filePath);
    srand((unsigned)time(NULL));
    for (int j=0;j<1000;j++) { //加随机数量 从0.1B往上加
            //随机生成n
            n = rand()%1000000;
            //忽略文件前n行
            for(int m=0;m<n;m++)
            {
                getline(inputFile,encryptedPath);
            }
            //读取第n行的文件路径
            getline(inputFile,encryptedPath);
            //存入vector
            allFilePaths.push_back(encryptedPath);
            //清空文件流
            inputFile.clear();
            //文件流指针回到文件头
            inputFile.seekg(0, ios::beg);
    }
    if (!inputFile) {
        cerr << "Error opening input file: " << filePath << endl;
    }
    inputFile.close();
    cout << "1.获取当前进程的 PID" <<endl;
    cout << "2.打印内存占用信息" <<endl;
    cout << "3.生成随机路径" <<endl;
    cout << "4.导入数据库" <<endl;
    cout << "5.查询文件信息" <<endl;
    cout << "6.删除数据库" <<endl;
    cout << "7.多线程查数据库" <<endl;
    cout << "-1.退出" <<endl;
    cout << "请输入：" <<endl;
    cin >> choose;
    cout <<endl;
    while(choose!=-1)
    {
        switch(choose)
        {
            case 1:
                // 获取当前进程的 PID
                pid = getpid();
                // 打印当前进程的 PID
                cout << "当前进程的 PID 是：" << pid << endl;
                break;
            case 2:
                printMemoryUsage();
                break;
            case 3:
                allFilePaths.clear();
                // 打开保存文件信息的文本文件,生成随机路径
                inputFile.open(filePath);
                srand((unsigned)time(NULL));
                for (int j=0;j<1000;j++) { //加随机数量 从0.1B往上加
                        //随机生成n
                        n = rand()%1000000;
                        //忽略文件前n行
                        for(int m=0;m<n;m++)
                        {
                            getline(inputFile,encryptedPath);
                        }
                        //读取第n行的文件路径
                        getline(inputFile,encryptedPath);
                        //存入vector
                        allFilePaths.push_back(encryptedPath);
                        //清空文件流
                        inputFile.clear();
                        //文件流指针回到文件头
                        inputFile.seekg(0, ios::beg);
                }
                if (!inputFile) {
                    cerr << "Error opening input file: " << filePath << endl;
                }
                inputFile.close();
                break;
            case 4:
                cout << "请输入待导入用户ID范围low和high:" <<endl;
                cin >> low >>high;
                gettimeofday(&t1,NULL);
                for(int i=low;i<=high;i++)
                {
                    cout<<"user"<<i<<endl;
                    infoFilePath = "../txt/file_info4.txt";
                    // 读取文件信息并存入B+树
                    readFileInfoFromMap(infoFilePath, mt_wrapper, i);
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(1000000*(high-low+1)) << "s" << endl;
                break;
            case 5:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入每个用户的查询路径个数：" <<endl;
                cin >> k;
                sync();
                //遍历查询filename中的所有文件路径
                gettimeofday(&t1,NULL);
                for(int i=low;i<=high;i++)
                {
                    for(int j=0;j<k;j++)
                    {
                        //查询文件信息
                        readfromLeafNode(mt_wrapper,generateID(allFilePaths[j],i));
                    }
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
                break;
            case 6:
                // 删除 log 目录下所有文件
                deleteAllFilesInDirectory(logPath);    
                break;
            case 7:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入每个用户的查询路径个数：" <<endl;
                cin >> k;
                cout << "请输入要开启的线程数：" << std::endl;
                cin >> numThreads;
                sync();
                // 获取开始时间
                gettimeofday(&t1, NULL);
                threads.clear();
                numTasks=(high - low + 1) * k;
                // 计算每个线程需要处理的任务数量
                tasksPerThread = numTasks / numThreads;
                // 循环创建线程执行查询操作
                for (int t = 0; t < numThreads; ++t) {
                    threads.emplace_back([low,k,numTasks, tasksPerThread, numThreads, t, &mt_wrapper,&allFilePaths]() { // 在捕获列表中添加 mt_wrapper
                    // 计算该线程负责的任务范围
                    int startTaskIndex = t * tasksPerThread;
                    int endTaskIndex = (t == numThreads - 1) ? numTasks - 1 : startTaskIndex + tasksPerThread - 1;

                    for (int taskIndex = startTaskIndex; taskIndex <= endTaskIndex; ++taskIndex) {
                        // 计算当前任务所属的用户ID和文件路径索引
                        int userID = low + taskIndex / k;
                        int filePathIndex = taskIndex % k;
                        readfromLeafNode(mt_wrapper, generateID(allFilePaths[filePathIndex], userID));
                    }             
                    });
                }
                // 等待所有线程执行完毕
                for (auto& thread : threads) {
                    thread.join();
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
                break;
            default:
                cout << "输入不合法，请重新输入" <<endl;
        }
        cout <<endl;
        cout << "1.获取当前进程的 PID" <<endl;
        cout << "2.打印内存占用信息" <<endl;
        cout << "3.生成随机路径" <<endl;
        cout << "4.导入数据库" <<endl;
        cout << "5.查询文件信息" <<endl;
        cout << "6.删除数据库" <<endl;
        cout << "7.多线程查数据库" <<endl;
        cout << "-1.退出" <<endl;
        cout << "请输入：" <<endl;
        cin >> choose;
    }
    deleteAllFilesInDirectory(logPath);  
    return 0;
}    

