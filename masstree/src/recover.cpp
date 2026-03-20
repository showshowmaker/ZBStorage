#define _GNU_SOURCE
#include <unordered_set>
#include <set>
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
#include <atomic>
#define BIG_CONSTANT(x) (x##LLU)
using namespace std;
namespace fs = filesystem;
#define pagesize 64
#define keysize 16
#define valuesize 234
#define userfilenum 1000000
constexpr int leafnum = (pagesize * 1024 - sizeof(uint8_t)) / (keysize + valuesize);
using sizetype = std::conditional_t<(leafnum >= 256), uint16_t, uint8_t>;
#define sizeofsize sizeof(sizetype)//16
#define leafnum ((pagesize*1024-sizeofsize)/(keysize+valuesize))//262
#define leafcount ((userfilenum+leafnum-1)/leafnum)//3817
#define emptynum (pagesize*1024-leafnum*(keysize+valuesize)-sizeofsize)

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
    void wait() {
        while(!m_queue.empty())
        {
            //休眠0.05s
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            //cout<<"wait"<<endl;
        }
        //cout<<"wait end"<<endl;
        // 等待所有线程完成任务
        shutdown();
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
//string logPath1 = "/mnt/bigdata/shot";//"../log" //"/mnt/NVMe/log"
// string logPath2 = "/mnt/md0/fff";
// string logPath3 = "/mnt/md0/fff";
string logPath1 = "/mnt/target/bplustreetest_test/log_data.bin";
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
string generateID(const string& filepath, uint64_t uid) {
    // 获取 filepath 中的父目录路径和文件名
    size_t pos = filepath.find_last_of('/');
    string parent_dir = filepath.substr(0, pos); // 父目录路径
    string filename = filepath.substr(pos + 1); // 文件名
    // 对父目录路径和文件名分别进行 MurmurHash2 算法散列
    uint64_t hash_parent_dir = MurmurHash64A(parent_dir.c_str(), parent_dir.length());
    uint64_t hash_filename = MurmurHash64A(filename.c_str(), filename.length());
    // 创建缓冲区来保存拼接后的字节流
    string result(24, '\0');
    // 将 uid 拷贝到缓冲区的开头，占用 8 个字节
    memcpy(&result[0], &uid, 8);
    char * ptr = &result[0]+8;
    memcpy(ptr, &hash_parent_dir, sizeof(hash_parent_dir));
    ptr += sizeof(hash_parent_dir);
    memcpy(ptr, &hash_filename, sizeof(hash_filename));
    return result;
}
struct FileInfo {
    string key;
    char type;
    uint64_t size;       // 文件大小
    time_t createTime;   // 文件创建时间
    time_t lastModifiedTime;  // 文件最近一次修改时间
    time_t lastAccessTime;    // 文件最近一次访问时间
    // 定义 < 运算符用于比较
    bool operator<(const FileInfo& other) const {
        return key < other.key;
    }
};
FileInfo fileInfos[userfilenum];
void readvectorfromtxt(const string& filePath)
{
    ifstream inputFile(filePath);
    if (!inputFile) {
        cerr << "Error opening input file: " << filePath << endl;
        return;
    }
    FileInfo fileInfo;
    string encryptedPath;
    string key;
    for(int i=0;i<userfilenum;i++)
    {
        getline(inputFile, encryptedPath);
        key = generateID(encryptedPath,1);
        //fileInfo.key删掉key的8个字节
        fileInfo.key=key.substr(8);
        getline(inputFile, encryptedPath);
        inputFile >> fileInfo.type >> fileInfo.size >> fileInfo.createTime >> fileInfo.lastModifiedTime >> fileInfo.lastAccessTime;
        inputFile.ignore(); // 忽略换行符
        fileInfos[i]=fileInfo;
    }
    inputFile.close();
    sort(fileInfos, fileInfos+userfilenum, [](const FileInfo& a, const FileInfo& b) {
        return a.key < b.key;
    });
}
int fd1;
// 读取磁盘操作函数
void readDisk(uint64_t found_address,char* buffer) {
    if (!buffer) {
        cerr << "Error allocating memory for buffer." << endl;
        return;
    }
    ssize_t bytesRead;
    bytesRead = pread(fd1, buffer, pagesize*1024, found_address*pagesize*1024);
    if (bytesRead == -1) {
        cerr << "Error reading file: "  << " - " << strerror(errno) << endl;
        free(buffer);
        return;
    }
}
bool readfromLeafNode(MasstreeWrapper& masstree_wrapper, const string& key) {
    // 创建一个向量来存储扫描结果
    vector<pair<string,uint64_t>> result;
    masstree_wrapper.scan(key, 1, result);
    if (result.empty()) {
        cout << "Leaf node not found for key: " << key << endl;
        return false;
    } else {
        //比较key的前四个字节与result[0]的前8个字节是否一样
        if(key.substr(0,8)!=result[0].first.substr(0,8))
        {
            cout << "Uid not right, key: " << key << endl;
            return false;
        }
        uint64_t oldnum = result[0].second;
        char *buffer = (char*)aligned_alloc(4096, 1024*pagesize);
        // 从磁盘中读取叶子节点的数据到 buffer 中
        readDisk(oldnum, buffer);
        return true;
        //cout<<"oldnum:"<<oldnum<<endl;
        //buffer的最后sizeofsize个字节存储size
 /*       sizetype size;
        memcpy(&size, buffer + 1024*pagesize-sizeofsize, sizeofsize);
        //cout<<"size:"<<(int)size<<endl;
        // 假设数据格式为键值对依次存储
        int low=0;
        int high=size-1;
        int mid;
        string key2=key.substr(8);
        const char* keyCStr=key2.c_str();
        while (low <= high) {
                mid = low + (high - low) / 2;
                int compareResult = memcmp(buffer + mid * (keysize+valuesize), keyCStr, keysize);
                if (compareResult == 0) {
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
        //cin>>mid;
        cout << "Value not found for key: " << key << endl;
        return false;*/
    }
}

void insertallfile(MasstreeWrapper& mt_wrapper,uint64_t uid)
{
    uint32_t nownumEntries;
    uint32_t ssize;
    sizetype size;
    string key(8, '\0');
    memcpy(&key[0], &uid, 8);
    int k=leafnum-1;
    mt_wrapper.thread_init(uid);
    for(int i=0;i<leafcount-1;i++)
    {
        mt_wrapper.insert(key+fileInfos[k].key,(uid-1)*leafcount+i);
        k+=leafnum;
    }
    mt_wrapper.insert(key+fileInfos[userfilenum-1].key,(uid-1)*leafcount+leafcount-1); 
}
void recover(MasstreeWrapper mt_wrapper,uint64_t firstuid,uint64_t lastuid)
{
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    ThreadPool pool(60);
    pool.init();
    for(int u=firstuid;u<=lastuid;u++)
    {
        pool.submit(insertallfile,ref(mt_wrapper),u);
    }
    pool.wait();
    gettimeofday(&t2,NULL);
    timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    cout << "total recover time:" << timeuse << "s" << endl;
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
void randomread(MasstreeWrapper& mt_wrapper,uint64_t low,uint64_t high,int k)
{
    for(int i=0;i<k;i++)
    {
        uint64_t uid=rand()%(high-low+1)+low;
        int kid=rand()%userfilenum;
        string key(8, '\0');
        memcpy(&key[0], &uid, 8);
        key+=fileInfos[kid].key;
        readfromLeafNode(mt_wrapper, key);
    }
}
void singleuserread(MasstreeWrapper& mt_wrapper,uint64_t uid,int k)
{
    for(int i=0;i<k;i++)
    {
        int kid=rand()%userfilenum;
        string key(8, '\0');
        memcpy(&key[0], &uid, 8);
        key+=fileInfos[kid].key;
        readfromLeafNode(mt_wrapper, key);
    }
}
int main() {
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    MasstreeWrapper mt_wrapper;
    int choose;
    string encryptedPath;
    int n;
    uint64_t low;
    uint64_t high;
    int k;
    pid_t pid;
    int tasksPerThread;
    int usrsPerTime;
    int numThreads;
    int numTasks;
    int remainingTasks;
    int startTaskIndex;
    int endTaskIndex;
    int tasksToProcess;
    string key(8, '\0');
    ifstream inputFile;
    const string filePath =  "../txt/filename.txt";
    string infoFilePath = "../txt/file_info.txt";
    vector<thread> threads;
    readvectorfromtxt(infoFilePath);
    recover(mt_wrapper,1,100);
    cout << "1.打印内存占用信息" <<endl;
    cout << "2.纯随机单线程查询" <<endl;
    cout << "3.单个用户内随机单线程查询" <<endl;
    cout << "4.纯随机多线程查询" <<endl;
    cout << "5.单个用户内随机多线程查询" <<endl;
    cout << "-1.退出" <<endl;
    cout << "请输入：" <<endl;
    cin >> choose;
    cout <<endl;
    while(choose!=-1)
    {
        switch(choose)
        {
            case 1:
                printMemoryUsage();
                break;
            case 2:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入待查询路径个数：" <<endl;
                cin >> k;
                sync();
                //遍历查询filename中的所有文件路径
                gettimeofday(&t1,NULL);
                fd1=open(logPath1.c_str(),O_RDONLY | O_DIRECT);
                randomread(mt_wrapper,low,high,k);
                close(fd1);
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/k << "s" << endl;
                break;
            case 3:
                cout << "请输入待查询用户ID:" <<endl;
                cin >> low;
                cout << "请输入待查询路径个数：" <<endl;
                cin >> k;
                sync();
                //遍历查询filename中的所有文件路径
                gettimeofday(&t1,NULL);
                fd1=open(logPath1.c_str(),O_RDONLY | O_DIRECT);
                singleuserread(mt_wrapper,low,k);
                close(fd1);
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/k << "s" << endl;
                break;
            case 4:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入待查询路径个数：" <<endl;
                cin >> k;
                cout << "请输入要开启的线程数：" << endl;
                cin >> numThreads;
                sync();
                // 获取开始时间
                gettimeofday(&t1, NULL);

                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/k << "s" << endl;
                break;
            case 5:
                cout << "请输入待查询用户ID:" <<endl;
                cin >> low;
                cout << "请输入待查询路径个数：" <<endl;
                cin >> k;
                cout << "请输入要开启的线程数：" << endl;
                cin >> numThreads;
                sync();
                // 获取开始时间
                gettimeofday(&t1, NULL);

                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/k << "s" << endl;
                break;
            default:
                cout << "输入不合法，请重新输入" <<endl;
        }
        cout <<endl;
        cout << "1.打印内存占用信息" <<endl;
        cout << "2.纯随机单线程查询" <<endl;
        cout << "3.单个用户内随机单线程查询" <<endl;
        cout << "4.纯随机多线程查询" <<endl;
        cout << "5.单个用户内随机多线程查询" <<endl;
        cout << "-1.退出" <<endl;
        cout << "请输入：" <<endl;
        cin >> choose;
    } 
    return 0;
}    