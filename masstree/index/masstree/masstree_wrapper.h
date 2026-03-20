#pragma once

#include <iostream>
#include <random>
#include <vector>
#include <thread>

#include <pthread.h>

#include "config.h"
#include "compiler.hh"

#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_print.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "string.hh"



class key_unparse_unsigned {
public:
    static int unparse_key(Masstree::key<uint64_t> key, char* buf, int buflen) {
        return snprintf(buf, buflen, "%" PRIu64, key.ikey());
    }
};

class MasstreeWrapper {
public:
    static constexpr uint64_t insert_bound = 0xfffff; //0xffffff;
    struct table_params : public Masstree::nodeparams<15,15> {
        typedef uint64_t value_type;
        typedef Masstree::value_print<value_type> value_print_type;
        typedef threadinfo threadinfo_type;
        typedef key_unparse_unsigned key_unparse_type;
        static constexpr ssize_t print_max_indent_depth = 12;
    };

    typedef Masstree::Str Str;
    typedef Masstree::basic_table<table_params> table_type;
    typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
    typedef Masstree::tcursor<table_params> cursor_type;
    typedef Masstree::leaf<table_params> leaf_type;
    typedef Masstree::internode<table_params> internode_type;

    typedef typename table_type::node_type node_type;
    typedef typename unlocked_cursor_type::nodeversion_value_type nodeversion_value_type;

    struct Scanner {
      const int cnt;
      //std::vector<table_params::value_type> &vec;
      std::vector<std::pair<std::string,table_params::value_type>> &vec;
      Scanner(int cnt, std::vector<std::pair<std::string,table_params::value_type>> &v)
          : cnt(cnt), vec(v) {
        vec.reserve(cnt);
      }

      template <typename SS, typename K>
      void visit_leaf(const SS &, const K &, threadinfo &) {}

      bool visit_value(Str key, table_params::value_type val, threadinfo &) {
        vec.push_back({key,val});
        if (vec.size() == cnt) {
          return false;
        }
        return true;
      }
    };

    static __thread typename table_params::threadinfo_type *ti;

    MasstreeWrapper() {
        this->table_init();
    }

    void table_init() {
        if (ti == nullptr)
            ti = threadinfo::make(threadinfo::TI_MAIN, -1);
        table_.initialize(*ti);
        key_gen_ = 0;
    }

    void keygen_reset() {
        key_gen_ = 0;
    }

    static void thread_init(int thread_id) {
        if (ti == nullptr)
            ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
    }

    void insert(Str key, uint64_t value) {
        cursor_type lp(table_, key);
        bool found = lp.find_insert(*ti);
        lp.value() = value;
        fence();
        lp.finish(1, *ti);
    }

    bool search(Str key, uint64_t &value) {
        bool found = table_.get(key, value, *ti);
        return found;
    }

    void scan(Str key, int cnt, std::vector<std::pair<std::string,uint64_t>> &vec) {
        Scanner scanner(cnt, vec);
        table_.scan(key, true, scanner, *ti);
    }
    void rscan(Str key, int cnt, std::vector<std::pair<std::string,uint64_t>> &vec) {
        Scanner scanner(cnt, vec);
        table_.rscan(key, true, scanner, *ti);
    }


    bool remove(Str key) {
        cursor_type lp(table_, key);
        bool found = lp.find_locked(*ti);
        lp.finish(-1, *ti);
        return true;
    }

private:
    table_type table_;
    uint64_t key_gen_;
    static bool stopping;
    static uint32_t printing;
};

