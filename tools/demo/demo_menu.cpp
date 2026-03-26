#include "demo_menu.h"

#include <algorithm>
#include <iostream>

namespace zb::demo {

void RenderMenu(const std::string& title, const std::vector<MenuActionSpec>& actions) {
    std::cout << "\n========================================\n";
    std::cout << " " << title << '\n';
    std::cout << "========================================\n";
    for (const auto& action : actions) {
        std::cout << action.id << ") " << action.title;
        if (!action.description.empty()) {
            std::cout << "  - " << action.description;
        }
        std::cout << '\n';
    }
    std::cout << "\n输入格式: <序号> key=value key=value\n";
    std::cout << "示例: 5 namespace=demo-ns generation=gen-001 file_count=100000000\n";
}

const MenuActionSpec* FindAction(const std::vector<MenuActionSpec>& actions, const std::string& token) {
    auto equals = [&](const MenuActionSpec& action) {
        if (action.id == token) {
            return true;
        }
        return std::find(action.aliases.begin(), action.aliases.end(), token) != action.aliases.end();
    };

    auto it = std::find_if(actions.begin(), actions.end(), equals);
    return it == actions.end() ? nullptr : &(*it);
}

} // namespace zb::demo
