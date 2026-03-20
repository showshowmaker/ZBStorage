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
#include <sys/file.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fstream>
#include <algorithm>
#include <thread>
#include <future>
#include <unistd.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#include "masstree_wrapper.h" 
#include <filesystem>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <utility>
#define BIG_CONSTANT(x) (x##LLU)
using namespace std;
namespace fs = filesystem;
#define pagesize 4
#define keysize 20
#define valuesize 236
#define filesize 128
#define leafnum pagesize*1024/(keysize+valuesize)
template <typename T>
class SafeQueue
{
private:
    std::queue<T> m_queue; //利用模板函数构造队列
    std::mutex m_mutex; // 访问互斥信号量
public:
    SafeQueue() {}
    SafeQueue(SafeQueue &&other) {}
    ~SafeQueue() {}
    bool empty() // 返回队列是否为空
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 互斥信号变量加锁，防止m_queue被改变
        return m_queue.empty();
    }
    int size()
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 互斥信号变量加锁，防止m_queue被改变
        return m_queue.size();
    }
    // 队列添加元素
    void enqueue(T &t)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.emplace(t);
    }
    // 队列取出元素
    bool dequeue(T &t)
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 队列加锁
        if (m_queue.empty())
            return false;
        t = std::move(m_queue.front()); // 取出队首元素，返回队首元素值，并进行右值引用
        m_queue.pop(); // 弹出入队的第一个元素
        return true;
    }
};
class ThreadPool
{
private:
    class ThreadWorker // 内置线程工作类
    {
    private:
        int m_id; // 工作id
        ThreadPool *m_pool; // 所属线程池
    public:
        // 构造函数
        ThreadWorker(ThreadPool *pool, const int id) : m_pool(pool), m_id(id)
        {
        }
        // 重载()操作
        void operator()()
        {
            std::function<void()> func; // 定义基础函数类func
            bool dequeued; // 是否正在取出队列中元素
            while (!m_pool->m_shutdown)
            {
                {
                    // 为线程环境加锁，互访问工作线程的休眠和唤醒
                    std::unique_lock<std::mutex> lock(m_pool->m_conditional_mutex);
                    // 如果任务队列为空，阻塞当前线程
                    if (m_pool->m_queue.empty())
                    {
                        m_pool->m_conditional_lock.wait(lock); // 等待条件变量通知，开启线程
                    }
                    // 取出任务队列中的元素
                    dequeued = m_pool->m_queue.dequeue(func);
                }
                // 如果成功取出，执行工作函数
                if (dequeued)
                    func();
            }
        }
    };
    bool m_shutdown; // 线程池是否关闭
    SafeQueue<std::function<void()>> m_queue; // 执行函数安全队列，即任务队列
    std::vector<std::thread> m_threads; // 工作线程队列
    std::mutex m_conditional_mutex; // 线程休眠锁互斥变量
    std::condition_variable m_conditional_lock; // 线程环境锁，可以让线程处于休眠或者唤醒状态
public:
    // 线程池构造函数
    ThreadPool(const int n_threads = 4)
        : m_threads(std::vector<std::thread>(n_threads)), m_shutdown(false)
    {
    }
    ThreadPool(const ThreadPool &) = delete;
    ThreadPool(ThreadPool &&) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;
    ThreadPool &operator=(ThreadPool &&) = delete;
    // Inits thread pool
    void init()
    {
        for (int i = 0; i < m_threads.size(); ++i)
        {
            m_threads.at(i) = std::thread(ThreadWorker(this, i)); // 分配工作线程
        }
    }
    // Waits until threads finish their current task and shutdowns the pool
    void shutdown()
    {
        m_shutdown = true;
        m_conditional_lock.notify_all(); // 通知，唤醒所有工作线程
        for (int i = 0; i < m_threads.size(); ++i)
        {
            if (m_threads.at(i).joinable()) // 判断线程是否在等待
            {
                m_threads.at(i).join(); // 将线程加入到等待队列
            }
        }
    }
    // Submit a function to be executed asynchronously by the pool
    template <typename F, typename... Args>
    auto submit(F &&f, Args &&...args) -> std::future<decltype(f(args...))>
    {
        // Create a function with bounded parameter ready to execute
        std::function<decltype(f(args...))()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...); // 连接函数和参数定义，特殊函数类型，避免左右值错误
        // Encapsulate it into a shared pointer in order to be able to copy construct
        auto task_ptr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);
        // Warp packaged task into void function
        std::function<void()> warpper_func = [task_ptr]()
        {
            (*task_ptr)();
        };
        // 队列通用安全封包函数，并压入安全队列
        m_queue.enqueue(warpper_func);
        // 唤醒一个等待中的线程
        m_conditional_lock.notify_one();
        // 返回先前注册的任务指针
        return task_ptr->get_future();
    }
};
class FileHandlePool {
public:

    int openFile(const string& filePath, int flags) {
        {
            std::shared_lock<std::shared_mutex> readLock(handlesMut);
            auto it = handles.find(filePath);
            if (it != handles.end()) {
                return it->second;
            }
        }
        int fd = ::open(filePath.c_str(), flags, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            perror("Error opening file");
            return -1;
        }
        {
            std::unique_lock<std::shared_mutex> writeLock(handlesMut);
            handles[filePath] = fd;
        }
        return fd;
    }
private:
    unordered_map<string,int> handles;
    shared_mutex handlesMut;
};
FileHandlePool pool;
void setReadLock(int fd, off_t offset, off_t len) {
    struct flock lock;
    lock.l_type = F_RDLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = offset;
    lock.l_len = len;
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLKW, &lock) == -1) {
        perror("fcntl");
        // 处理错误
    } 
}

void setWriteLock(int fd, off_t offset, off_t len) {
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = offset;
    lock.l_len = len;
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLKW, &lock) == -1) {
        perror("fcntl");
        // 处理错误
    } 
}

void unlock(int fd, off_t offset, off_t len) {
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = offset;
    lock.l_len = len;
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLK, &lock) == -1) {
        perror("fcntl");
        // 处理错误
    } 
}
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
string logPath = "/mnt/NVMe/logp";//"../log" //"/mnt/NVMe/log"


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
    mtx.lock();
    uint64_t nownumEntries = numEntries++;
    mtx.unlock();
    uint64_t currentFileIndex = nownumEntries / (filesize * 1024 / pagesize);
    int offset = nownumEntries % (filesize * 1024 / pagesize) * pagesize*1024;
    string fileName = logPath + "/log_" + to_string(currentFileIndex) + ".txt";
    int fd = pool.openFile(fileName, O_RDWR | O_CREAT);
    if (fd == -1) {
        cerr << "Error opening file: " << fileName << endl;
        return -1;
    }
    setWriteLock(fd, offset, (keysize+valuesize)*size);
    ssize_t bytesWritten = pwrite(fd, buffer, (keysize+valuesize)*size, offset);
    if (bytesWritten == -1) {
        cerr << "Error writing file: " << fileName << endl;
        unlock(fd, offset, (keysize+valuesize)*size);
        return -1;
    }
    unlock(fd, offset, (keysize+valuesize)*size);
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
    int fd = pool.openFile(fileName, O_RDWR | O_CREAT);
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

void readAndUpdateLeafNode(MasstreeWrapper& masstree_wrapper, const string& key, char* value) {
    std::hash<std::thread::id> hasher;
    masstree_wrapper.thread_init(hasher(std::this_thread::get_id()));
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
            //int fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            int fd = pool.openFile(fileName, O_RDWR | O_CREAT);
            if (fd == -1) {
                cerr << "Error opening file: " << fileName << " - " << strerror(errno) << endl;
                return;
            }
            setWriteLock(fd, alignedOffset+(keysize+valuesize)*size, keysize+valuesize);
            ssize_t bytesWritten = pwrite(fd, kv, keysize+valuesize, alignedOffset+(keysize+valuesize)*size);
            if (bytesWritten == -1) {
                cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                unlock(fd, alignedOffset+(keysize+valuesize)*size, keysize+valuesize);
                return;
            }
            unlock(fd, alignedOffset+(keysize+valuesize)*size, keysize+valuesize);
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
        //int fd = open(fileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        int fd = pool.openFile(fileName, O_RDWR | O_CREAT);
        if (fd == -1) {
            cerr << "Error opening file: " << fileName << " - " << strerror(errno) << endl;
            return;
        }
        char buffer[(keysize+valuesize)*(size+2)];
        // 移动文件指针到指定位置
        // 锁定文件以进行读取（共享锁）
        // if (flock(fd, LOCK_SH) == -1) {
        //     std::cerr << "Error locking file for read: " << fileName << " - " << strerror(errno) << std::endl;
        //     return;
        // }
        setReadLock(fd, alignedOffset, (keysize+valuesize)*size);
        // if (lseek(fd, alignedOffset, SEEK_SET) == -1) {

        //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
        //     //close(fd);
        //     flock(fd, LOCK_UN);  // 解锁文件
        //     return;
        // }
        // // 使用普通读取方式读取文件内容到缓冲区
        // ssize_t bytesRead = read(fd, buffer+(keysize+valuesize), (keysize+valuesize)*size);
        ssize_t bytesRead = pread(fd, buffer+(keysize+valuesize), (keysize+valuesize)*size, alignedOffset);
        //cout<<"bytesRead:"<<bytesRead <<endl;
        if (bytesRead == -1) {
            cerr << "Error reading file: " << fileName << " - " << strerror(errno) << endl;
            //close(fd);
            unlock(fd, alignedOffset, (keysize+valuesize)*size);
            return;
        }
        unlock(fd, alignedOffset, (keysize+valuesize)*size);
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
                    // 锁定文件以进行写入（独占锁）
                    setWriteLock(fd, alignedOffset+(keysize+valuesize)*(mid-1)+keysize, valuesize);
                    // if (lseek(fd, alignedOffset+(keysize+valuesize)*(mid-1), SEEK_SET) == -1) {
                    //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                    //     //close(fd);
                    //     //cin>>kkkk;
                    //     flock(fd, LOCK_UN);
                    //     return;
                    // }
                    // ssize_t bytesWritten = write(fd, ptr, valuesize);
                    // 使用 pwrite() 写入数据到指定偏移量处
                    ssize_t bytesWritten = pwrite(fd, ptr, valuesize, alignedOffset+(keysize+valuesize)*(mid-1)+keysize);
                    //close(fd);
                    // 检查写入是否成功
                    if (bytesWritten == -1) {
                        cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                        unlock(fd, alignedOffset+(keysize+valuesize)*(mid-1)+keysize, valuesize);
                        return;
                    }
                    // 解锁文件
                    unlock(fd, alignedOffset+(keysize+valuesize)*(mid-1)+keysize, valuesize);
                    return;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }//low-1个小于  size-low+1个大于
            if(low<=leafnum/2)
            {
                // 锁定文件以进行写入（独占锁）
                // if (flock(fd, LOCK_EX) == -1) {
                //     std::cerr << "Error locking file for write: " << fileName << " - " << strerror(errno) << std::endl;
                //     return;
                // }
                setWriteLock(fd, alignedOffset, (leafnum-leafnum/2)*(keysize+valuesize));
                // if (lseek(fd, alignedOffset, SEEK_SET) == -1) {
                //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                //     //close(fd);
                //     //cin>>kkkk;
                //     flock(fd, LOCK_UN);  // 解锁文件
                //     return;
                // }
                // ssize_t bytesWritten = write(fd, buffer + (leafnum/2+1) * (keysize+valuesize), (leafnum-leafnum/2)*(keysize+valuesize));
                // 使用 pwrite() 写入数据到指定偏移量处
                ssize_t bytesWritten = pwrite(fd, buffer + (leafnum/2+1) * (keysize+valuesize), (leafnum-leafnum/2)*(keysize+valuesize), alignedOffset);
                //close(fd);
                // 检查写入是否成功
                if (bytesWritten == -1) {
                    cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                    unlock(fd, alignedOffset, (leafnum-leafnum/2)*(keysize+valuesize));
                    return;
                }
                // 解锁文件
                unlock(fd, alignedOffset, (leafnum-leafnum/2)*(keysize+valuesize));
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
                    // 锁定文件以进行写入（独占锁）
                // if (flock(fd, LOCK_EX) == -1) {
                //     std::cerr << "Error locking file for write: " << fileName << " - " << strerror(errno) << std::endl;
                //     return;
                // }
                setWriteLock(fd, alignedOffset, (leafnum-leafnum/2+1)*(keysize+valuesize)); 
                // if (lseek(fd, alignedOffset, SEEK_SET) == -1) {
                //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                //     //close(fd);
                //     //cin>>kkkk;
                //     flock(fd, LOCK_UN);
                //     return;
                // }
                // ssize_t bytesWritten = write(fd, buffer + leafnum/2 * (keysize+valuesize), (leafnum-leafnum/2+1)*(keysize+valuesize));
                // 使用 pwrite() 写入数据到指定偏移量处
                ssize_t bytesWritten = pwrite(fd, buffer + leafnum/2 * (keysize+valuesize), (leafnum-leafnum/2+1)*(keysize+valuesize), alignedOffset);
                //close(fd);
                // 检查写入是否成功
                if (bytesWritten == -1) {
                    cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                    unlock(fd, alignedOffset, (leafnum-leafnum/2+1)*(keysize+valuesize));
                    return;
                }
                // 解锁文件
                unlock(fd, alignedOffset, (leafnum-leafnum/2+1)*(keysize+valuesize));
                leafNodeCount = leafnum-leafnum/2+1; 
                combinedData = (oldnum << 8) | leafNodeCount;
                masstree_wrapper.insert(result[0].first,combinedData);
                return;
            }
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
                    // 锁定文件以进行写入（独占锁）
                    // if (flock(fd, LOCK_EX) == -1) {
                    //     std::cerr << "Error locking file for write: " << fileName << " - " << strerror(errno) << std::endl;
                    //     return;
                    // }
                    setWriteLock(fd, alignedOffset+(keysize+valuesize)*(mid-1)+keysize, valuesize);
                    // if (lseek(fd, alignedOffset+(keysize+valuesize)*(mid-1), SEEK_SET) == -1) {
                    //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
                    //     //close(fd);
                    //     //cin>>kkkk;
                    //     flock(fd, LOCK_UN);
                    //     return;
                    // }
                    // ssize_t bytesWritten = write(fd, ptr, valuesize);
                    ssize_t bytesWritten = pwrite(fd, ptr, valuesize, alignedOffset+(keysize+valuesize)*(mid-1)+keysize);
                    //close(fd);
                    // 检查写入是否成功
                    if (bytesWritten == -1) {
                        cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                        unlock(fd, alignedOffset+(keysize+valuesize)*(mid-1)+keysize, valuesize);
                        return;
                    }
                    // 解锁文件
                    unlock(fd, alignedOffset+(keysize+valuesize)*(mid-1)+keysize, valuesize);
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
            // 锁定文件以进行写入（独占锁）
            // if (flock(fd, LOCK_EX) == -1) {
            //     std::cerr << "Error locking file for write: " << fileName << " - " << strerror(errno) << std::endl;
            //     return;
            // }
            setWriteLock(fd, alignedOffset+(keysize+valuesize)*(low-1), (size-low+2)*(keysize+valuesize));
            // if (lseek(fd, alignedOffset+(keysize+valuesize)*(low-1), SEEK_SET) == -1) {
            //     cerr << "Error seeking file: " << fileName << " - " << strerror(errno) << endl;
            //     //close(fd);
            //     //cin>>kkkk;
            //     flock(fd, LOCK_UN);
            //     return;
            // }
            // ssize_t bytesWritten = write(fd, ptr, (size-low+2)*(keysize+valuesize));//size-low+2
            ssize_t bytesWritten = pwrite(fd, ptr, (size-low+2)*(keysize+valuesize), alignedOffset+(keysize+valuesize)*(low-1));
            //close(fd);
            // 检查写入是否成功
            if (bytesWritten == -1) {
                cerr << "Error writing file: " << fileName << " - " << strerror(errno) << endl;
                unlock(fd, alignedOffset+(keysize+valuesize)*(low-1), (size-low+2)*(keysize+valuesize));
                return;
            }
            // 解锁文件
            unlock(fd, alignedOffset+(keysize+valuesize)*(low-1), (size-low+2)*(keysize+valuesize));
            uint8_t leafNodeCount = size+1; // 新建的叶子节点个数为 1
            uint64_t combinedData = (oldnum << 8) | leafNodeCount;
            masstree_wrapper.insert(result[0].first,combinedData);
            return;
        }
    }
}
void readFileInfoFromMap(int numThreads,const string& filePath, MasstreeWrapper& mt_wrapper, uint32_t uid,uint32_t lastuid)
{
    //mt_wrapper.thread_init(uid);
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
        num=10000;
                            ThreadPool tpool(numThreads);
                            //cout<<numThreads<<endl;
                    tpool.init();
                    // 用于存储任务的future对象
    std::vector<std::future<void>> futures;
        while (getline(inputFile, encryptedPath)&&num--) { //读单独拿出来 
           //cout<<num<<endl;
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
                //readAndUpdateLeafNode(mt_wrapper, key, value);
                //tpool.submit(readAndUpdateLeafNode,ref(mt_wrapper),key,value);
                futures.push_back(tpool.submit(readAndUpdateLeafNode, std::ref(mt_wrapper), key, value));
            }
        }
        // 等待所有任务完成
    for (auto& f : futures) {
        f.get();
    }
    tpool.shutdown();
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
    const int total_insertions = 10000;
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
            n = rand()%10000;
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
    // 创建输出文件流对象并打开文件
    ofstream outputFile("../txt/output1000pool.txt");
    if (!outputFile.is_open()) {
        cerr << "Error opening output file!" << endl;
        return 1;
    }
    // 将输出重定向到文件
    streambuf *coutbuf = cout.rdbuf(); // 保存 cout 的缓冲区指针
    cout.rdbuf(outputFile.rdbuf()); // 将 cout 的缓冲区指针重定向到 outputFile
    for(numThreads=1;numThreads<=100;numThreads++)
    {
        MasstreeWrapper mt_wrapper;
        cout<<"numThreads:"<<numThreads<<endl;
        low=1;
        high=100;
        gettimeofday(&t1,NULL);
        readFileInfoFromMap(numThreads,infoFilePath, mt_wrapper, low,high);
        gettimeofday(&t2,NULL);
        timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
        cout << "total time:" << timeuse << "s" << endl;
        cout << "average find time:" << timeuse/(high-low+1) << "s" << endl<<endl;
                for(int i=low;i<=high;i++)
                {
                    for(int j=0;j<100;j++)
                    {
                        //查询文件信息
                        readfromLeafNode(mt_wrapper,generateID(allFilePaths[j],i));
                    }
                }
        deleteAllFilesInDirectory(logPath); 
    }
    // 恢复标准输出流的缓冲区指针
    cout.rdbuf(coutbuf);
    outputFile.close();
    // cout << "1.获取当前进程的 PID" <<endl;
    // cout << "2.打印内存占用信息" <<endl;
    // cout << "3.生成随机路径" <<endl;
    // cout << "4.导入数据库" <<endl;
    // cout << "5.查询文件信息" <<endl;
    // cout << "6.删除数据库" <<endl;
    // cout << "7.多线程查数据库" <<endl;
    // cout << "-1.退出" <<endl;
    // cout << "请输入：" <<endl;
    // cin >> choose;
    // cout <<endl;
    // while(choose!=-1)
    // {
    //     switch(choose)
    //     {
    //         case 1:
    //             // 获取当前进程的 PID
    //             pid = getpid();
    //             // 打印当前进程的 PID
    //             cout << "当前进程的 PID 是：" << pid << endl;
    //             break;
    //         case 2:
    //             printMemoryUsage();
    //             break;
    //         case 3:
    //             allFilePaths.clear();
    //             // 打开保存文件信息的文本文件,生成随机路径
    //             inputFile.open(filePath);
    //             srand((unsigned)time(NULL));
    //             for (int j=0;j<1000;j++) { //加随机数量 从0.1B往上加
    //                     //随机生成n
    //                     n = rand()%10000;
    //                     //忽略文件前n行
    //                     for(int m=0;m<n;m++)
    //                     {
    //                         getline(inputFile,encryptedPath);
    //                     }
    //                     //读取第n行的文件路径
    //                     getline(inputFile,encryptedPath);
    //                     //存入vector
    //                     allFilePaths.push_back(encryptedPath);
    //                     //清空文件流
    //                     inputFile.clear();
    //                     //文件流指针回到文件头
    //                     inputFile.seekg(0, ios::beg);
    //             }
    //             if (!inputFile) {
    //                 cerr << "Error opening input file: " << filePath << endl;
    //             }
    //             inputFile.close();
    //             break;
    //         case 4:
    //             cout << "请输入待导入用户ID范围low和high:" <<endl;
    //             cin >> low >>high;
    //             cout << "请输入要开启的线程数：" << endl;
    //             cin >> numThreads;
    //             gettimeofday(&t1,NULL);
    //             {
    //                 readFileInfoFromMap(numThreads,infoFilePath, mt_wrapper, low, high); 
    //             }
    //             gettimeofday(&t2,NULL);
    //             timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    //             cout << "total time:" << timeuse << "s" << endl;
    //             cout << "average find time:" << timeuse/(1000000*(high-low+1)) << "s" << endl;
    //             break;
    //         case 5:
    //             cout << "请输入待查询用户ID范围low和high:" <<endl;
    //             cin >> low >>high;
    //             cout << "请输入每个用户的查询路径个数：" <<endl;
    //             cin >> k;
    //             sync();
    //             //遍历查询filename中的所有文件路径
    //             gettimeofday(&t1,NULL);
    //             for(int i=low;i<=high;i++)
    //             {
    //                 for(int j=0;j<k;j++)
    //                 {
    //                     //查询文件信息
    //                     readfromLeafNode(mt_wrapper,generateID(allFilePaths[j],i));
    //                 }
    //             }
    //             gettimeofday(&t2,NULL);
    //             timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    //             cout << "total time:" << timeuse << "s" << endl;
    //             cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
    //             break;
    //         case 6:
    //             // 删除 log 目录下所有文件
    //             deleteAllFilesInDirectory(logPath);    
    //             break;
    //         case 7:
    //             cout << "请输入待查询用户ID范围low和high:" <<endl;
    //             cin >> low >>high;
    //             cout << "请输入每个用户的查询路径个数：" <<endl;
    //             cin >> k;
    //             cout << "请输入要开启的线程数：" << endl;
    //             cin >> numThreads;
    //             sync();
    //             // 获取开始时间
    //             gettimeofday(&t1, NULL);
    //             threads.clear();
    //             numTasks=(high - low + 1) * k;
    //             // 计算每个线程需要处理的任务数量
    //             tasksPerThread = numTasks / numThreads;
    //             // 循环创建线程执行查询操作
    //             for (int t = 0; t < numThreads; ++t) {
    //                 threads.emplace_back([low,k,numTasks, tasksPerThread, numThreads, t, &mt_wrapper,&allFilePaths]() { // 在捕获列表中添加 mt_wrapper
    //                 // 计算该线程负责的任务范围
    //                 int startTaskIndex = t * tasksPerThread;
    //                 int endTaskIndex = (t == numThreads - 1) ? numTasks - 1 : startTaskIndex + tasksPerThread - 1;

    //                 for (int taskIndex = startTaskIndex; taskIndex <= endTaskIndex; ++taskIndex) {
    //                     // 计算当前任务所属的用户ID和文件路径索引
    //                     int userID = low + taskIndex / k;
    //                     int filePathIndex = taskIndex % k;
    //                     readfromLeafNode(mt_wrapper, generateID(allFilePaths[filePathIndex], userID));
    //                 }             
    //                 });
    //             }
    //             // 等待所有线程执行完毕
    //             for (auto& thread : threads) {
    //                 thread.join();
    //             }
    //             gettimeofday(&t2,NULL);
    //             timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    //             cout << "total time:" << timeuse << "s" << endl;
    //             cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
    //             break;
    //         default:
    //             cout << "输入不合法，请重新输入" <<endl;
    //     }
    //     cout <<endl;
    //     cout << "1.获取当前进程的 PID" <<endl;
    //     cout << "2.打印内存占用信息" <<endl;
    //     cout << "3.生成随机路径" <<endl;
    //     cout << "4.导入数据库" <<endl;
    //     cout << "5.查询文件信息" <<endl;
    //     cout << "6.删除数据库" <<endl;
    //     cout << "7.多线程查数据库" <<endl;
    //     cout << "-1.退出" <<endl;
    //     cout << "请输入：" <<endl;
    //     cin >> choose;
    // }
    //deleteAllFilesInDirectory(logPath);  
    return 0;
}    

