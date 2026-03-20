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
#define filesize 128
#define max_open_files 10000
#define leafnum pagesize*1024/(keysize+valuesize)

class FileHandlePool {
public:
    explicit FileHandlePool(size_t maxHandles)
        : maxHandles(maxHandles) {}

    // 打开文件并加入句柄池
    int openFile(const string& filePath, int flags) {
        //lock_guard<mutex> lock(mutex);

        // 首先检查句柄池中是否已有此文件
        auto it = handles.find(filePath);
        if (it != handles.end()) {
            // 已存在，移动到链表尾部（表示最近使用）
            //cout<<"已存在"<<endl;
            //cout<<"fd:"<<(*it->second).first<<endl;
            //recentlyUsed.splice(recentlyUsed.end(), recentlyUsed, it->second);
            //打印最近访问链表
            //cout<<"最近访问链表:"<<endl;
            // for(auto it=recentlyUsed.begin();it!=recentlyUsed.end();it++)
            // {
            //     cout<<it->second<<"   "<<it->first<<endl;
            // }
            return it->second;
        }
        // 检查句柄池是否已满
        // if (handles.size() >= maxHandles) {
        //     //cout<<"满了"<<endl;
        //     closeOldestFile();
        // }
        mtx.lock();
        int fd = ::open(filePath.c_str(), flags, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            perror("Error opening file");
            return -1;
        }
        // cout<<"打开文件"<<endl;
        // cout<<"fd:"<<fd<<endl;
        // cout<<"filePath:"<<filePath<<endl;


        // 将新文件描述符添加到句柄池和最近使用列表
        // recentlyUsed.emplace_back(fd, filePath);
        // mtx.lock();
        handles[filePath] = fd;
        mtx.unlock();
            // cout<<"最近访问链表:"<<endl;
            // for(auto it=recentlyUsed.begin();it!=recentlyUsed.end();it++)
            // {
            //     cout<<it->second<<"   "<<it->first<<endl;
            // }
        return fd;
    }

    // 关闭并从池中移除文件
    // void closeFile(const string& filePath) {
    //     //lock_guard<mutex> lock(mutex);
    //     auto it = handles.find(filePath);
    //     if (it != handles.end()) {
    //         close(it->second->first);
    //         recentlyUsed.erase(it->second);
    //         handles.erase(it);
    //     }
    // }

private:
    // 关闭最旧的文件
    // void closeOldestFile() {
    //     if (recentlyUsed.empty()) throw runtime_error("Recently used list is empty");
    //     //cout<<"关闭最旧的文件"<<endl;
    //     auto oldestIter = recentlyUsed.begin();
    //     int fdToRemove = oldestIter->first;
    //     auto handleIter = handles.find(oldestIter->second);
    //     if (handleIter != handles.end()) {
    //         handles.erase(handleIter);
    //     }
    //     recentlyUsed.erase(oldestIter);
    //     //cout<<"关闭的文件fd:"<<fdToRemove<<endl;
    //     close(fdToRemove);
    // }
    unordered_map<string, int> handles;
    // unordered_map<string, list<pair<int, string>>::iterator> handles;
    // list<pair<int, string>> recentlyUsed; // LRU列表
    //mutex mutex;
    size_t maxHandles;
    mutex mtx;
};
FileHandlePool pool(max_open_files);
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
string logPath = "/mnt/md0/log5";//"../log" //"/mnt/NVMe/log"
bool cover_happen = false;

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
bool allocateSpaceForFiles( int numFiles, off_t size) {
    for (int i = 0; i < numFiles; ++i) {
        string filePath = logPath + "/log_" + to_string(i) + ".txt";
        int fd = open(filePath.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd == -1) {
            cerr << "Failed to open file " << filePath << endl;
            continue;
        }
        
        if (fallocate(fd, 0, 0, size) == -1) {
            cerr << "Failed to allocate space for file " << filePath << endl;
            close(fd);
            continue;
        }

        close(fd);
        cout << "Allocated " << size / (1024 * 1024) << "MB space for file " << filePath << endl;
    }
    return true;
}
// 分配磁盘空间函数
//初始化mtx
mutex mtx;
uint64_t allocateDiskSpace(char* buffer,int size) {
    // 计算当前文件索引和偏移量
    mtx.lock();
    uint64_t nownumEntries = numEntries++;
    mtx.unlock();
    uint64_t currentFileIndex = nownumEntries / (filesize * 1024 / pagesize);
    int offset = nownumEntries % (filesize * 1024 / pagesize) * pagesize*1024;
    // 构造文件名
    string fileName = logPath + "/log_" + to_string(currentFileIndex) + ".txt";
    // 打开文件
    int fd = open(fileName.c_str(), O_WRONLY | O_CREAT , S_IRUSR | S_IWUSR);
    //int fd = pool.openFile(fileName, O_WRONLY | O_CREAT);
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
    return nownumEntries;
}
// 读取磁盘操作函数
void readDisk(uint64_t found_address,char* buffer) {
    // 计算文件索引和偏移量
    uint64_t fileIndex = found_address / (filesize * 1024 / pagesize);
    int alignedOffset = found_address % (filesize * 1024 / pagesize) * pagesize*1024;
    // 构造文件名
    string fileName = logPath + "/log_" + to_string(fileIndex) + ".txt";
    // 打开文件
    //int fd = open(fileName.c_str(), O_RDONLY | O_DIRECT);
    int fd = pool.openFile(fileName, O_RDONLY | O_DIRECT);
    if (fd == -1) {
        cerr << "Error opening file: " << fileName << " - " << strerror(errno) << endl;
        return;
    }
    if (!buffer) {
        cerr << "Error allocating memory for buffer." << endl;
        //close(fd);
        return;
    }
    // 使用pread()直接读取文件内容到缓冲区
    ssize_t bytesRead = pread(fd, buffer, pagesize*1024, alignedOffset);
    if (bytesRead == -1) {
        cerr << "Error reading file: " << fileName << " - " << strerror(errno) << endl;
        //close(fd);
        free(buffer);
        return;
    }
    // 关闭文件并释放缓冲区
    //close(fd);
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
                    //memcpy(value, buffer + mid * (keysize+valuesize)+keysize, len);

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
        //cin>>mid;
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

bool readfromLeafNode2(MasstreeWrapper& masstree_wrapper, const string& key) {
    // 创建一个向量来存储扫描结果
    vector<pair<string,uint64_t>> result;
    masstree_wrapper.scan(key, 1, result);
    if (result.empty()) {
        //cout << "Leaf node not found for key: " << key << endl;
		return false;
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
                    return true;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        // 释放内存
        free(buffer);
        //cout << "Value not found for key: " << key << endl;
		return false;
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
            //mtx.lock();
            uint64_t newnum=allocateDiskSpace(kv,1);
            //mtx.unlock();
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
            uint64_t fileIndex = oldnum / (filesize * 1024 / pagesize);
            int alignedOffset = oldnum % (filesize * 1024 / pagesize) * 1024*pagesize;
            // 构造文件名
            string fileName = logPath + "/log_" + to_string(fileIndex) + ".txt";
            // 打开文件
            int fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            //int fd = pool.openFile(fileName, O_RDWR | O_CREAT);
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
            //mtx.lock();
            masstree_wrapper.remove(result[0].first);
            masstree_wrapper.insert(key,combinedData);
            //mtx.unlock();
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
        uint64_t fileIndex = oldnum / (filesize * 1024 / pagesize);
        int alignedOffset = oldnum % (filesize * 1024 / pagesize) * pagesize*1024;
        // 构造文件名
        string fileName = logPath + "/log_" + to_string(fileIndex) + ".txt";
        // 打开文件
        int fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        //int fd = pool.openFile(fileName, O_RDWR | O_CREAT);
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
        if (bytesRead == -1) {
            cerr << "Error reading file: " << fileName << " - " << strerror(errno) << endl;
            close(fd);
            int kkkk;
            cin>>kkkk;
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
void readFileInfoFromMap(const string& filePath, MasstreeWrapper& mt_wrapper, uint32_t uid,uint32_t lastuid)
{
    mt_wrapper.thread_init(uid);
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
    string key;
    // 将各个字段逐个拷贝到value中
    int num;
    // for(int i=uid;i<=lastuid;i++)
    // {
        num=1000000;
        while (getline(inputFile, encryptedPath)&&num--) { //读单独拿出来 
            inputFile >> nameLength >> type >> size >> createTime >> lastModifiedTime >> lastAccessTime;
            inputFile.ignore(); // 忽略换行符
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
            for(uint32_t i=uid;i<=lastuid;i++)
            {
                // 生成ID
                string key = generateID(encryptedPath,i);
                // 读取文件信息
                readAndUpdateLeafNode(mt_wrapper, key, value);
            }
            // key = generateID(encryptedPath,i);
            // readAndUpdateLeafNode(mt_wrapper, key, value);
        }
    //     inputFile.clear();
    //     inputFile.seekg(0, ios::beg);
    // }
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
    numEntries = 0;
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
void insert(MasstreeWrapper& masstree_wrapper, const string& key, uint64_t value) {
    masstree_wrapper.insert(key, value);
}
void multiThreadInsert(MasstreeWrapper& mt_wrapper, int num_threads) {
    vector<thread> threads;
    const int total_insertions = 1000000;
    const int insertions_per_thread = total_insertions / num_threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&mt_wrapper, t, insertions_per_thread]() {
            int start_idx = t * insertions_per_thread;
            int end_idx = start_idx + insertions_per_thread;
            mt_wrapper.thread_init(start_idx);
            for (int i = start_idx; i < end_idx; ++i) {
                insert(mt_wrapper, to_string(i), i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

class bitset
{
public:
	bitset(size_t bitCount)
		:_bitCount(bitCount)
	{
		_bitset.resize(bitCount / 32 + 1);
		for(size_t i = 0; i < _bitset.size(); ++i)
		{
			_bitset[i] = 0;
		}
	}

	// 每个字节用0和1记录某个数字存在状态
	// 把x所在的数据在位图的那一位变成1
	void set(int x)
	{
		if (x > _bitCount) return;

		int index = x / 32;// x是第index个整数
		int pos = x % 32;// x是滴index个整数的第pos个位

		_bitset[index] |= (1 << pos);
	}
	// 把x所在的数据在位图的那一位变成0
	void reset(int x)
	{
		if (x > _bitCount) return;

		int index = x / 32;// x是第index个整数
		int pos = x % 32;// x是滴index个整数的第pos个位

		_bitset[index] &= ~(1 << pos);
	}
	// 判断x是否存在
	bool test(int x)
	{
		if (x > _bitCount) return false;

		int index = x / 32;// x是第index个整数
		int pos = x % 32;// x是滴index个整数的第pos个位

		return _bitset[index] & (1 << pos);
	}
private:
	vector<int> _bitset;
	int _bitCount;// 开num比特位的空间
};

template<class T>
struct Hash
{
	const T& operator()(const T& key)
	{
		return key;
	}
};

// BKDRHash
struct BKDRHash
{
	size_t operator()(const string& str)
	{
		register size_t hash = 0;
		for (size_t i = 0; i < str.length(); ++i)
		{
			hash = hash * 131 + str[i];
		}

		return hash;
	}
};

// SDBHash
struct SDBHash
{
	size_t operator()(const string str)
	{
		register size_t hash = 0;
		for (size_t i = 0; i < str.length(); ++i)
		{
			hash = 65599 * hash + str[i];
			//hash = (size_t)ch + (hash << 6) + (hash << 16) - hash;  
		}
		return hash;
	}
};

// RSHash
struct RSHash
{
	size_t operator()(const string str)
	{
		register size_t hash = 0;
		size_t magic = 63689;
		for (size_t i = 0; i < str.length(); ++i)
		{
			hash = hash * magic + str[i];
			magic *= 378551;
		}
		return hash;
	}
};

template<class T = string, class Hash1 = BKDRHash, class Hash2 = SDBHash, class Hash3 = RSHash>
class BloomFilter
{
public:
	// 布隆过滤器的长度 近似等于4.3~5倍插入元素的个数
	// 但这里取 20倍
	BloomFilter(size_t size)
		:_bs(20*size)
		, _N(20 * size)
	{}

	void set(const T& x)
	{
		size_t index1 = Hash1()(x) % _N;
		size_t index2 = Hash2()(x) % _N;
		size_t index3 = Hash3()(x) % _N;

		_bs.set(index1);
		_bs.set(index2);
		_bs.set(index3);
	}
	// 不支持删除，因为删除某个元素可能会导致其它的元素的状态被修改
	bool IsInBloomFilter(const T& x)
	{
		size_t index1 = Hash1()(x) % _N;
		size_t index2 = Hash2()(x) % _N;
		size_t index3 = Hash3()(x) % _N;

		return _bs.test(index1)
			&& _bs.test(index2)
			&& _bs.test(index3);// 可能会误报，判断在是不准确的，判断不在是准确的
	}

private:
	bitset _bs;
	size_t _N;// 能够映射元素个数 
};

void TestBloomFilter() //测试程序
{
	BloomFilter<string> bf(100);

	bf.set("douyin");
	bf.set("kuaishou");
	bf.set("pass cet6");
	bf.set("aabb");


	cout << bf.IsInBloomFilter("pass cet6") << endl;
	cout << bf.IsInBloomFilter("kuaishou") << endl;
	cout << bf.IsInBloomFilter("douyin") << endl;
	cout << bf.IsInBloomFilter("abab") << endl;
}


//重点！！！偷懒没有把uid加进去！！！用的时候别搞错了！！！（但是参数给准备好了）
//判断是否重命名并返回处理后的路径
bool isRenamed(const string& path, BloomFilter<string>& bf, unordered_map<string, string>& rename_map, string& oldPath, uint32_t uid)
{
    if(bf.IsInBloomFilter(path))
    {
        auto it = rename_map.find(path);
        if(it != rename_map.end())
        {
            oldPath = it->second;
            return true;
        }
		else{
			oldPath = path;
		}
        return false;
    }
	else{
		oldPath = path;
	}
	return false;
}

//路径样例：/0R54C6pZOkl/yIU1TveY3ve/0oVgctYYSVi
void rename_helper(const string& path, BloomFilter<string>& bf, unordered_map<string, string>& rename_map, string& oldPath, uint32_t uid)
{
	std::string left_path = path.substr(1);
    size_t pos = left_path.find('/');
	oldPath.clear();
	while(pos != string::npos)
	{
		oldPath += '/';
		oldPath += left_path.substr(0, pos);
		isRenamed(oldPath, bf, rename_map, oldPath, uid);
		left_path = left_path.substr(pos + 1);
		pos = left_path.find('/');
	}
	oldPath += '/';
	oldPath += left_path.substr(0, pos);
	isRenamed(oldPath, bf, rename_map, oldPath, uid);
}

void my_rename(const string&oldPath, const string& path, BloomFilter<string>& bf, unordered_map<string, string>& rename_map, MasstreeWrapper& masstree_wrapper, uint32_t uid)
{
    //补充查询oldPath对应文件是否已经在索引树中
	string ori_oldPath, ori_newPath;
    bool isOldPathRenamed, isNewPathRenamed;
	//初步排查新旧路径是否存在，并完成对路径的处理
	vector<std::thread> threads;
    threads.emplace_back([oldPath, &bf, &rename_map, &ori_oldPath, &uid, &isOldPathRenamed]() { 
        isRenamed(oldPath, bf, rename_map, ori_oldPath, uid);      
    });
	threads.emplace_back([path, &bf, &rename_map, &ori_newPath, &uid, &isNewPathRenamed]() { 
        isRenamed(path, bf, rename_map, ori_newPath, uid);      
    });
    // 等待所有线程执行完毕
    for (auto& thread : threads) {
        thread.join();
    }
    // isRenamed(oldPath, bf, rename_map, ori_oldPath, uid); 
    // isRenamed(path, bf, rename_map, ori_newPath, uid);

    //查找被重命名文件是否存在
	bool flag_old, flag_new;
	threads.clear();
	threads.emplace_back([&masstree_wrapper, &ori_oldPath, &uid, &flag_old]() { 
        flag_old = readfromLeafNode2(masstree_wrapper, generateID(ori_oldPath, uid));      
    });
	threads.emplace_back([&masstree_wrapper, &ori_newPath, &uid, &flag_new]() { 
        flag_new = readfromLeafNode2(masstree_wrapper, generateID(ori_newPath, uid));    
    });
    // 等待所有线程执行完毕
    for (auto& thread : threads) {
        thread.join();
    }

    if(flag_old && flag_new)
	{
		cout << "是否确定替换？（y/n）" << endl;
		cout << "Warning: 目录将递归替换" << endl;
        char choice;
        cin >> choice;
        if(choice == 'n')
        {
			cout << "取消重命名操作" << endl;
            return;
        }
        else{
            cover_happen = true;
        }
	}
	else if(!flag_old){
		cout << "被重命名的文件不存在" << endl;
		return ;
	}
	//flag_old == 1,flag_new == 0 
    bf.set(ori_newPath);
    rename_map[ori_newPath] = ori_oldPath;
	//cout << "重命名成功" << endl;
}

void clear_rename_buffer(unordered_map<string, string>& rename_map, BloomFilter<string>& bf)
{
	rename_map.clear();
	bf = BloomFilter<string>(100000); //没有析构，可能会有内存泄漏
    cover_happen = false;
}

bool readthroughRNB(const string& path,unordered_map<string, string>& rename_map, BloomFilter<string>& bf, MasstreeWrapper& masstree_wrapper, uint32_t uid)
{
	// struct timeval t1,t2;
	// double timeuse;
	// gettimeofday(&t1,NULL);
	string oldPath;
	rename_helper(path, bf, rename_map, oldPath, uid);
	// if(oldPath == path){
	// 	gettimeofday(&t2,NULL);
    //     timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
	// 	cout << "total time:" << timeuse << "s" << endl;
	// 	cout << "oldpath == path" << endl;
	// 	return false;
	// }
	//cout << "path -> oldpath " << path << " " << oldPath << endl;
	// gettimeofday(&t2,NULL);
    // timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    // cout << "total time:" << timeuse << "s" << endl;
	// return readfromLeafNode2(masstree_wrapper, generateID(oldPath, uid));
    readfromLeafNode(masstree_wrapper, generateID(oldPath, uid));
	return true;
}

int main() {
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    MasstreeWrapper mt_wrapper;
    int choose;
    vector<string> allFilePaths;
	vector<string> renamedPaths;
    unordered_map<string, string> rename_map; //重命名哈希表
    BloomFilter<string> bf(100000);	//布隆过滤器
    string encryptedPath;
    int n;
    int low;
    int high;
    int k;
    pid_t pid;
    int tasksPerThread;
    int numThreads;
    int numTasks;
    int remainingTasks;
    int startTaskIndex;
    int endTaskIndex;
    int tasksToProcess;
    ifstream inputFile;
    const string filePath =  "../txt/filename.txt";
    string infoFilePath = "../txt/file_info.txt";
    deleteAllFilesInDirectory(logPath); 
    allFilePaths.clear();
    vector<thread> threads;
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
    // // 创建输出文件流对象并打开文件
    // ofstream outputFile("../txt/output1000_yes.txt");
    // if (!outputFile.is_open()) {
    //     cerr << "Error opening output file!" << endl;
    //     return 1;
    // }
    // // 将输出重定向到文件
    // streambuf *coutbuf = cout.rdbuf(); // 保存 cout 的缓冲区指针
    // cout.rdbuf(outputFile.rdbuf()); // 将 cout 的缓冲区指针重定向到 outputFile
    // for(numThreads=1;numThreads<=100;numThreads++)
    // {
    //     MasstreeWrapper mt_wrapper;
    //     low=1;
    //     high=1000;
    //     gettimeofday(&t1,NULL);
    //     threads.clear();
    //     if (!allocateSpaceForFiles(10, filesize * 1024 * 1024)) {
    //         cerr << "Failed to allocate space for files" << endl;
    //     }
    //     numTasks=high - low + 1;//100
    //     // 计算每个线程需要处理的任务数量
    //     tasksPerThread = numTasks / numThreads;//50
    //     remainingTasks = numTasks % numThreads;//0
    //     startTaskIndex=low;
    //     cout<<"numThreads:"<<numThreads<<endl;
    //     for (int t = 0; t < numThreads; ++t) {
    //         tasksToProcess = tasksPerThread + (t < remainingTasks ? 1 : 0);
    //         endTaskIndex = startTaskIndex + tasksToProcess - 1;
    //         cout<<startTaskIndex<<"~"<<endTaskIndex<<endl;
    //         threads.emplace_back([startTaskIndex, endTaskIndex, &mt_wrapper,&infoFilePath]() { // 在捕获列表中添加 mt_wrapper
    //         // 计算该线程负责的任务范围
    //             readFileInfoFromMap(infoFilePath, mt_wrapper, startTaskIndex, endTaskIndex);         
    //         });
    //         startTaskIndex = endTaskIndex + 1;
    //     }
    //     // 等待所有线程执行完毕
    //     for (auto& thread : threads) {
    //         thread.join();
    //     }
    //     gettimeofday(&t2,NULL);
    //     timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    //     cout << "total time:" << timeuse << "s" << endl;
    //     cout << "average find time:" << timeuse/(high-low+1) << "s" << endl<<endl;
    //     // int k;
    //     // cout<<"请输入："<<endl;
    //     // cin>>k;
    //     deleteAllFilesInDirectory(logPath); 
    // }
    // // 恢复标准输出流的缓冲区指针
    // cout.rdbuf(coutbuf);
    // outputFile.close();
    cout << "1.获取当前进程的 PID" <<endl;
    cout << "2.打印内存占用信息" <<endl;
    cout << "3.生成随机路径" <<endl;
    cout << "4.导入数据库" <<endl;
    cout << "5.查询文件信息" <<endl;
    cout << "6.删除数据库" <<endl;
    cout << "7.多线程查数据库" <<endl;
    cout << "8.重命名" <<endl;
	cout << "9.查找（检查重命名缓存）" <<endl;
    cout << "10.多线程查找（检查重命名缓存）" <<endl;
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
                cout << "请输入要开启的线程数：" << endl;
                cin >> numThreads;
                gettimeofday(&t1,NULL);
                threads.clear();
                numTasks=high - low + 1;//100
                // 计算每个线程需要处理的任务数量
                tasksPerThread = numTasks / numThreads;//1
                remainingTasks = numTasks % numThreads;//30
                // if (!allocateSpaceForFiles(500, filesize * 1024 * 1024)) {
                //     cerr << "Failed to allocate space for files" << endl;
                // }
                startTaskIndex=low;
                for (int t = 0; t < numThreads; ++t) {
                    tasksToProcess = tasksPerThread + (t < remainingTasks ? 1 : 0);
                    endTaskIndex = startTaskIndex + tasksToProcess - 1;
                    cout<<"startTastIndex:"<<startTaskIndex<<endl;
                    cout<<"endTaskIndex:"<<endTaskIndex<<endl;
                    threads.emplace_back([startTaskIndex, endTaskIndex, &mt_wrapper,&infoFilePath]() { // 在捕获列表中添加 mt_wrapper
                    // 计算该线程负责的任务范围
                        readFileInfoFromMap(infoFilePath, mt_wrapper, startTaskIndex, endTaskIndex);         
                    });
                    startTaskIndex = endTaskIndex + 1;
                }
                // 等待所有线程执行完毕
                for (auto& thread : threads) {
                    thread.join();
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
                cout << "请输入要开启的线程数：" << endl;
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
            case 8:
			    cout << "请输入要重命名的文件个数" << endl; //为测试方便，自动乘以用户数
				clear_rename_buffer(rename_map, bf);
                cin >> k;
                sync();
                //重命名filename中的前k个文件路径
                gettimeofday(&t1,NULL);
                for(int j=0;j<k;j++)
                {
                    //查询文件信息
					my_rename(allFilePaths[j],allFilePaths[j]+"_new",bf,rename_map,mt_wrapper,1);
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/k << "s" << endl;
				renamedPaths.clear();
				for(int j=0;j<k;j++)
                {
                    renamedPaths.push_back(allFilePaths[j]+"_new");
                }
				for(int j=k;j<1000;j++){
					renamedPaths.push_back(allFilePaths[j]);
				}
				break;
			case 9:
			    cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入每个用户的查询路径个数：" <<endl;
                cin >> k;
                cout << "是否存在替换情况(1:yes 0:no)" << endl;
                cin >> cover_happen;
                sync();
                //遍历查询filename中的所有文件路径
                gettimeofday(&t1,NULL);
                for(uint32_t i=low;i<=high;i++)
                {
                    for(int j=0;j<k;j++)
                    {
						// string& path = renamedPaths[j];
                        if(cover_happen){
                            readthroughRNB(renamedPaths[j], rename_map, bf, mt_wrapper, i);
                        }
                        else{
                            if(!readfromLeafNode2(mt_wrapper,generateID(renamedPaths[j],i))){
                                readthroughRNB(renamedPaths[j], rename_map, bf, mt_wrapper, i);
                            }
                        }
						// readthroughRNB(path, rename_map, bf, mt_wrapper, i);
                        //查询文件信息
						// bool flag_old, flag_new;
	                    // threads.clear();
	                    // threads.emplace_back([path, &rename_map, &bf, &mt_wrapper, &i, &flag_new]() { 
                        //     flag_new = readthroughRNB(path, rename_map, bf, mt_wrapper, i);      
                        // });
	                    // threads.emplace_back([&mt_wrapper, path, &i, &flag_old]() { 
                        //     flag_old = readfromLeafNode2(mt_wrapper, generateID(path, i));    
                        // });
                        // // 等待所有线程执行完毕
                        // for (auto& thread : threads) {
                        //     thread.join();
                        // }
						// if(!flag_old && !flag_new){
						// 	cout << "未找到目标文件" << endl;
						// }
						// if(!flag_new){
						// 	cout << "重命名路径未能找到对应文件" << endl;
						// }
                    }
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
				break;
            case 10:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入每个用户的查询路径个数：" <<endl;
                cin >> k;
                cout << "请输入要开启的线程数：" << std::endl;
                cin >> numThreads;
                cout << "是否存在替换情况(1:yes 0:no)" << endl;
                cin >> cover_happen;
                sync();
                // 获取开始时间
                gettimeofday(&t1, NULL);
                threads.clear();
                numTasks=(high - low + 1) * k;
                // 计算每个线程需要处理的任务数量
                tasksPerThread = numTasks / numThreads;
                // 循环创建线程执行查询操作
                for (int t = 0; t < numThreads; ++t) {
                    threads.emplace_back([low,k,numTasks, tasksPerThread, numThreads, t, &mt_wrapper,&renamedPaths, &bf, &rename_map]() { // 在捕获列表中添加 mt_wrapper
                    // 计算该线程负责的任务范围
                    int startTaskIndex = t * tasksPerThread;
                    int endTaskIndex = (t == numThreads - 1) ? numTasks - 1 : startTaskIndex + tasksPerThread - 1;

                    for (int taskIndex = startTaskIndex; taskIndex <= endTaskIndex; ++taskIndex) {
                        // 计算当前任务所属的用户ID和文件路径索引
                        int userID = low + taskIndex / k;
                        int filePathIndex = taskIndex % k;
                        //readfromLeafNode(mt_wrapper, generateID(renamedPaths[filePathIndex], userID));
                        if(cover_happen){
                            readthroughRNB(renamedPaths[filePathIndex], rename_map, bf, mt_wrapper, userID);
                        }
                        else{
                            if(!readfromLeafNode2(mt_wrapper, generateID(renamedPaths[filePathIndex], userID))){
                                readthroughRNB(renamedPaths[filePathIndex], rename_map, bf, mt_wrapper, userID);
                            }
                        }
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
		cout << "8.重命名" <<endl;
		cout << "9.查找（检查重命名缓存）" <<endl;
        cout << "10.多线程查找（检查重命名缓存）" <<endl;
        cout << "-1.退出" <<endl;
        cout << "请输入：" <<endl;
        cin >> choose;
    }
    //deleteAllFilesInDirectory(logPath);  
    return 0;
}    

