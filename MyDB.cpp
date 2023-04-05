#pragma warning(disable : 4996)
#include <iostream>
#include"Store.h"
using std::cout;

time_t StringToDatetime(std::string str)
{
    tm tm_;
    int year, month, day, hour, minute, second;
    year = atoi((str.substr(0, 4)).c_str());
    month = atoi((str.substr(4, 2)).c_str());
    day = atoi((str.substr(6, 2)).c_str());
    hour = atoi((str.substr(8, 2)).c_str());
    minute = atoi((str.substr(10, 2)).c_str());
    second = atoi((str.substr(12, 2)).c_str());

    tm_.tm_year = year - 1900;
    tm_.tm_mon = month - 1;
    tm_.tm_mday = day;
    tm_.tm_hour = hour;
    tm_.tm_min = minute;
    tm_.tm_sec = second;
    tm_.tm_isdst = 0;
    time_t t_ = mktime(&tm_);
    return t_;
}

bool GetWhere(Plan* plan, int& i, int&pos_tar, string& sql,string target) {
    if (target == "timestamp") {
        if (plan->flag_timestamp_ == 1) {
            cout << "已经指定过时间" << endl;
            return false;
        }
        for (; i < sql.length(); i++) {
            if (sql[i] != ' ') break;
        }
        string ch = sql.substr(i, 1);
        if (ch != "=") {
            cout << "未找到= 或 目前只支持等于查询不支持范围查询" << endl;
            return false;
        }
        i++;
        for (; i < sql.length(); i++) {
            if (sql[i] != ' ') break;
        }
        pos_tar = i;
        int length_time = 0;
        for (; i < sql.length(); i++) {
            if (sql[i] == ' ')break;
            length_time++;
        }
        string time = sql.substr(pos_tar, length_time);
        uint64_t timestamp = StringToDatetime(time);
        plan->SetTime(timestamp);
    }
    else {
        for (; i < sql.length(); i++) {
            if (sql[i] != ' ') break;
        }
        string ch = sql.substr(i, 1);
        if (ch != "=") {
            cout << "未找到= 或 目前只支持等于查询不支持范围查询" << endl;
            return false;
        }
        i++;
        for (; i < sql.length(); i++) {
            if (sql[i] != ' ') break;
        }
        pos_tar = i;
        int length_value = 0;
        for (; i < sql.length(); i++) {
            if (sql[i] == ' ')break;
            length_value++;
        }
        string value = sql.substr(pos_tar, length_value);
        plan->SetTagKey(target);
        plan->SetTagValue(value);
    }
}

int main()
{
    string sql;
    shared_ptr<Store> store = make_shared<Store>();
    if (!store->Init()) {
        cout << "初始化失败" << endl;
        return 0;
    }
    Plan* plan = new Plan;
    cout << "Hello!"<<endl;

    char space = ' ';
    int pos_begin = 0;
    while (1) {
        getline(cin, sql);
        if (sql.length() == 0) continue;
        
        pos_begin = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql[i] != space) break;
            pos_begin++;
        }

        int length_function = 0;
        for (int i = pos_begin; i < sql.length(); i++) {
            if (sql[i] == space) break;
            length_function++;
        }
        if (length_function == 0) continue;

        string function = sql.substr(pos_begin, length_function);   
        int pos_then = pos_begin + length_function;
        if (function == "show") {
            int length_tar = 0;
            int flag = 0;
            int tmp = pos_then;
            for (int i = tmp; i < sql.length(); i++) {
                if (sql[i] == space && flag == 0) {
                    pos_then++;
                    continue;
                }
                if (sql[i] == space && flag == 1) {
                    break;
                }
                length_tar++;
                flag = 1;
            }
            if (length_tar == 0) {
                cout << "请重新输入正确的语句(缺少show的目标)" << endl;
                continue;
            }
            string tar = sql.substr(pos_then, length_tar);
            if (pos_then+length_tar < sql.size()) {
                for (int i = pos_then + length_tar; i < sql.size(); i++) {
                    if (sql[i] != space) {
                        cout << "未能识别"<<tar<<"之后的字符串, 请重新输入" << endl;
                        continue;
                    }
                }
            }
            if (tar == "databases") {
                vector<string>databases;
                store->ShowDatabases(databases);
                cout << "共有" << databases.size() << "个" << endl;
                cout << "---------------------------------------------------" << endl;
                for (int i = 0; i < databases.size(); i++) {
                    cout << databases[i] << endl;
                }
                continue;
            }
            else if (tar == "shards") {
                vector<ShardInfo>shards;
                shards = store->ShowAllShardInfo();
                cout << "共有" << shards.size() << "个" << endl;
                cout << "---------------------------------------------------" << endl;
                cout << "ShardId\t\tOwner\t\tStart Time\t\tEnd Time\t\tDelete Time" << endl;
                for (int i = 0; i < shards.size(); i++) {
                    cout << shards[i].id_ << "\t\t" << shards[i].owner_ << "\t\t" <<
                        shards[i].start_time_.tm_year+1900 << "-"<< shards[i].start_time_.tm_mon+1 << "-" << shards[i].start_time_.tm_mday << "  " << shards[i].start_time_.tm_hour << ":" << shards[i].start_time_.tm_min << ":" << shards[i].start_time_ .tm_sec<< "\t\t"
                        << shards[i].end_time_.tm_year + 1900 << "-" << shards[i].end_time_.tm_mon+1 << "-" << shards[i].end_time_.tm_mday << "  " << shards[i].end_time_.tm_hour << ":" << shards[i].end_time_.tm_min << ":" << shards[i].end_time_.tm_sec << "\t\t"
                        << shards[i].delete_time_.tm_year + 1900 << "-" << shards[i].delete_time_.tm_mon+1 << "-" << shards[i].delete_time_.tm_mday << "  " << shards[i].delete_time_.tm_hour << ":" << shards[i].delete_time_.tm_min << ":" << shards[i].delete_time_.tm_sec << endl;
                }
                continue;
            }
            else {
                cout << "未找到与" << tar << "相关的语句" << endl;
                continue;
            }
        }
        else if (function == "create") {
            int length_tar = 0;
            int flag = 0;
            int pos_tar = pos_then;
            for (int i = pos_then; i < sql.length(); i++) {
                if (sql[i] == space && flag == 0) { 
                    pos_tar++;
                    continue;
                }
                if (sql[i] == space && flag == 1) break;
                flag = 1;
                length_tar++;
            }
            if (length_tar == 0) {
                cout << "请重新输入正确的语句(缺少创建的目标)" << endl;
                continue;
            }
            string tar = sql.substr(pos_tar, length_tar);
            if (tar == "database") {
                int length_database = 0;
                int flag = 0;
                int index = pos_tar + length_tar;
                for (int i = pos_tar + length_tar; i < sql.length(); i++) {
                    if (sql[i] == space && flag == 0) { 
                        index++;
                        continue; 
                    }
                    flag = 1;
                    length_database++;
                }
                string database = sql.substr(index, length_database);
                if (store->CreateNewDatabase(database)) {
                    cout << "OK!" << endl;
                }
                else {
                    cout << "此数据库已经存在!" << endl;
                }
                continue;
            }
            else {
                cout << "不支持创建" << tar << endl;
                continue;
            }
        }
        else if (function == "select") {
            plan->clear();
            int i = pos_begin + length_function;
            for (; i < sql.length(); i++) {
                if (sql[i] != ' ') break;
            }
            if (i == sql.length()) {
                getline(cin, sql);
                while (sql.length() == 0) getline(cin, sql);
                i = 0;
            }
            int pos_tar = i;
            int length_fieldkey = 0;
            for (; i < sql.length(); i++) {
                if (sql[i] == ',' || sql[i] == ' ')break;
                length_fieldkey++;
            }
            plan->select_.push_back(sql.substr(pos_tar, length_fieldkey));
            while (sql[i] == ',') {
                pos_tar += length_fieldkey + 1;
                length_fieldkey = 0;
                for (; i < sql.length(); i++) {
                    if (sql[i] == ',' || sql[i] == ' ')break;
                    length_fieldkey++;
                }
                plan->select_.push_back(sql.substr(pos_tar, length_fieldkey));
            }
            if (i == sql.length()) {
                getline(cin, sql);
                while (sql.length() == 0) getline(cin, sql);
                i = 0;
            }
            for (; i < sql.length(); i++) {
                if (sql[i] != ' ') break;
            }
            pos_tar = i;
            int length_from = 0;
            for (; i < sql.length(); i++) {
                if (sql[i] == ' ')break;
                length_from++;
            }
            string from = sql.substr(pos_tar, length_from);
            if (from == "from") {
                for (; i < sql.length(); i++) {
                    if (sql[i] != ' ') break;
                }
                pos_tar = i;
                int length_measurement = 0;
                for (; i < sql.length(); i++) {
                    if (sql[i] == ' ')break;
                    length_measurement++;
                }
                string measurement = sql.substr(pos_tar, length_measurement);
                plan->SetMeasurement(measurement);

                for (; i < sql.length(); i++) {
                    if (sql[i] != ' ') break;
                }
                if (i == sql.length()) break;
                
                bool flag_group = 0;
                bool flag_where = 0;
                int length = 0;
                pos_tar = i;
                for (; i < sql.length(); i++) {
                    if (sql[i] == ' ')break;
                    length++;
                }
                string where_or_group = sql.substr(pos_tar, length);
                string andorgroup;
                if (where_or_group == "where") {
                    bool flag = 1;
                    while (flag) {
                        for (; i < sql.length(); i++) {
                            if (sql[i] != ' ') break;
                        }
                        if (i == sql.length()) {
                            getline(cin, sql);
                            while (sql.length() == 0) getline(cin, sql);
                            i = 0;
                        }
                        for (; i < sql.length(); i++) {
                            if (sql[i] != ' ') break;
                        }
                        if (i == sql.length()) {
                            flag = 0;
                            cout << "请重新输入" << endl;
                            break;
                        }
                        pos_tar = i;
                        int length_tar = 0;
                        for (; i < sql.length(); i++) {
                            if (sql[i] == ' ')break;
                            length_tar++;
                        }
                        string target = sql.substr(pos_tar, length_tar);
                        if (!GetWhere(plan, i, pos_tar, sql, target)) {
                            flag = 0;
                            break;
                        }
                        for (; i < sql.length(); i++) {
                            if (sql[i] != ' ') break;
                        }
                        pos_tar = i;
                        if (i == sql.length())break;
                        int length_andorgroup = 0;
                        for (; i < sql.length(); i++) {
                            if (sql[i] == ' ')break;
                            length_andorgroup++;
                        }
                        andorgroup = sql.substr(pos_tar, length_andorgroup);
                        if (andorgroup == "groupby") break;
                        else if (andorgroup == "and")continue;
                        else {
                            cout << "输入错误AndOrGroupBy" << endl;
                            flag == 0;
                            break;
                        }
                    }
                    flag_where = 1;
                    if (flag == 0) continue;
                }
                else if (where_or_group == "groupby") {
                    for (; i < sql.length(); i++) {
                        if (sql[i] != ' ') break;
                    }
                    if (i == sql.length()) {
                        getline(cin, sql);
                        while (sql.length() == 0) getline(cin, sql);
                        i = 0;
                    }
                    for (; i < sql.length(); i++) {
                        if (sql[i] != ' ') break;
                    }
                    if (i == sql.length()) {
                        cout << "请重新输入" << endl;
                        continue;
                    }
                    pos_tar = i;
                    int length_tar = 0;
                    for (; i < sql.length(); i++) {
                        if (sql[i] == ' ')break;
                        length_tar++;
                    }
                    string target = sql.substr(pos_tar, length_tar);
                    plan->SetGroup(target);
                    flag_group = 1;
                }
                else {
                    cout << "输入错误(where_or_group)" << endl;
                    continue;
                }

                length = 0;
                pos_tar = i;
                if (i < sql.length()) {
                    for (; i < sql.length(); i++) {
                        if (sql[i] == ' ')break;
                        length++;
                    }
                    where_or_group = sql.substr(pos_tar, length);
                    if (where_or_group == "where") {
                        if (flag_where == 1) {
                            cout << "已经输入过where" << endl;
                            continue;
                        }
                        bool flag = 1;
                        while (flag) {
                            for (; i < sql.length(); i++) {
                                if (sql[i] != ' ') break;
                            }
                            if (i == sql.length()) {
                                getline(cin, sql);
                                while (sql.length() == 0) getline(cin, sql);
                                i = 0;
                            }
                            for (; i < sql.length(); i++) {
                                if (sql[i] != ' ') break;
                            }
                            if (i == sql.length()) {
                                flag = 0;
                                cout << "请重新输入" << endl;
                                break;
                            }
                            pos_tar = i;
                            int length_tar = 0;
                            for (; i < sql.length(); i++) {
                                if (sql[i] == ' ')break;
                                length_tar++;
                            }
                            string target = sql.substr(pos_tar, length_tar);
                            if (!GetWhere(plan, i, pos_tar, sql, target)) {
                                flag = 0;
                                break;
                            }
                            for (; i < sql.length(); i++) {
                                if (sql[i] != ' ') break;
                            }
                            pos_tar = i;
                            int length_andorgroup = 0;
                            for (; i < sql.length(); i++) {
                                if (sql[i] == ' ')break;
                                length_andorgroup++;
                            }
                            andorgroup = sql.substr(pos_tar, length_andorgroup);
                            if (andorgroup == "groupby") break;
                            else if (andorgroup == "and")continue;
                            else {
                                cout << "输入错误AndOrGroupBy" << endl;
                                flag == 0;
                                break;
                            }
                        }
                        flag_where = 1;
                        if (flag == 0) continue;
                    }
                    else if (where_or_group == "groupby") {
                        if (flag_group == 1) {
                            cout << "已经输入过groupby" << endl;
                            continue;
                        }
                        for (; i < sql.length(); i++) {
                            if (sql[i] != ' ') break;
                        }
                        if (i == sql.length()) {
                            getline(cin, sql);
                            while (sql.length() == 0) getline(cin, sql);
                            i = 0;
                        }
                        for (; i < sql.length(); i++) {
                            if (sql[i] != ' ') break;
                        }
                        if (i == sql.length()) {
                            cout << "请重新输入" << endl;
                            continue;
                        }
                        pos_tar = i;
                        int length_tar = 0;
                        for (; i < sql.length(); i++) {
                            if (sql[i] == ' ')break;
                            length_tar++;
                        }
                        string target = sql.substr(pos_tar, length_tar);
                        plan->SetGroup(target);
                        flag_group = 1;
                    }
                    else {
                        cout << "输出错误(where_or_group)" << endl;
                        continue;
                    }
                }

                if (plan->flag_timestamp_ == 0) {
                    vector<string>result_values;
                    vector<uint64_t>result_times;
                    result_values.reserve(4096);
                    result_times.reserve(4096);
                    vector<string> group_info;
                    store->SearchData(plan, result_values, result_times,group_info);
                    if (result_values.size() == 0) {
                        cout << "未找到结果" << endl;
                        continue;
                    }
                    if (plan->groupby_.empty()) {
                        int divide_value = plan->select_.size();
                        for (int i = 0; i < divide_value; i++) {
                            cout << plan->select_[i] << "\t\t";
                        }
                        cout << "time" << endl;
                        for (int i = 0; i < result_values.size() / divide_value; i++) {
                            if (result_values[i] == "000") {
                                continue;
                            }
                            for (int j = 0; j < divide_value; j++) {
                                cout << result_values[j * result_values.size() / divide_value + i] << "\t\t";
                            }
                            tm time = *gmtime((time_t*) & result_times[i]);
                            cout << time.tm_year + 1900 << "-" << time.tm_mon + 1 << "-" << time.tm_mday << " " << time.tm_hour << ":" << time.tm_min << ":" << time.tm_sec << endl;
                        }
                    }
                    else {
                        int divide_value = plan->select_.size();
                        cout << plan->groupby_ << "\t\t";
                        for (int i = 0; i < divide_value; i++) {
                            cout << plan->select_[i] << "\t\t";
                        }
                        cout << time << endl;
                        int index = 0;
                        for (int i = 0; i < result_values.size() / divide_value; i++) {
                            if (result_values[i] == "000") {
                                index++;
                                continue;
                            }
                            cout << group_info[index] << "\t\t";
                            for (int j = 0; j < divide_value; j++) {
                                cout << result_values[j * result_values.size() / divide_value + i] << "\t\t";
                            }
                            tm time = *gmtime((time_t*)&result_times[i]);
                            cout << time.tm_year + 1900 << "-" << time.tm_mon + 1 << "-" << time.tm_mday << " " << time.tm_hour << ":" << time.tm_min << ":" << time.tm_sec << endl;
                        }
                    }
                    
                }
                 else {
                    vector<string>result_values;
                    result_values.reserve(4096);
                    vector<string> group_info;
                    store->SearchDataByTime(plan, plan->timestamp_, result_values, group_info);
                    if (result_values.size() == 0) {
                        cout << "未找到结果" << endl;
                        continue;
                    }
                    if (plan->groupby_.empty()) {
                        int divide_value = plan->select_.size();
                        for (int i = 0; i < divide_value; i++) {
                            cout << plan->select_[i] << "\t\t";
                        }
                        cout << time << endl;
                        for (int i = 0; i < result_values.size() / divide_value; i++) {
                            if (result_values[i] == "000") {
                                continue;
                            }
                            for (int j = 0; j < divide_value; j++) {
                                cout << result_values[j * result_values.size() / divide_value + i] << "\t\t";
                            }
                        }
                    }
                    else {
                        int divide_value = plan->select_.size();
                        cout << plan->groupby_ << "\t\t";
                        for (int i = 0; i < divide_value; i++) {
                            cout << plan->select_[i] << "\t\t";
                        }
                        cout << time << endl;
                        int index = 0;
                        for (int i = 0; i < result_values.size() / divide_value; i++) {
                            if (result_values[i] == "000") {
                                index++;
                                continue;
                            }
                            cout << group_info[index] << "\t\t";
                            for (int j = 0; j < divide_value; j++) {
                                cout << result_values[j * result_values.size() / divide_value + i] << "\t\t";
                            }
                        }
                    }
                }
                cout << "完成" << endl;
                continue;
            }
            else {
                cout << "输入错误(from)" << endl;
                continue;
            }
            


        }
        //<measurement>[,<tag-key>=<tag-value>...] <field-key>=<field-value>[,<field2-key>=<field2-value>...] [timestamp]
        else if (function == "insert") {
            int i = pos_begin + length_function;
            for (; i < sql.length(); i++) {
                if (sql[i] != ' ') break;
            }
            if (i == sql.length()) {
                getline(cin, sql);
                while (sql.length() == 0) getline(cin, sql);
                i=0;
            }
            int pos_sentence = i;
            int length_measurement = 0;
            for (; i < sql.length(); i++) {
                if (sql[i] == ',')break;
                length_measurement++;
            }
            if (sql[i] != ',') {
                cout << "输入错误请重新输入(error:1)" << endl;
                continue;
            }
               
            string measurement = sql.substr(pos_sentence, length_measurement);

            int pos = pos_sentence + length_measurement + 1;
            int j = pos_sentence + length_measurement + 1;
            int length_tagkey = 0;

            vector<Tag> tag;
            int tag_num = 0;
            bool ERROR = false;
            //处理tag
            while (!ERROR) {
                pos = j;
                length_tagkey = 0;
                for (; j < sql.length(); j++) {
                    if (sql[j] == '=') break;
                    length_tagkey++;
                }
                if (sql[j] != '=') {
                    cout << "输入错误请重新输入(error:2)" << endl;
                    ERROR = true;
                    continue;
                }
                Tag new_tag;
                tag.push_back(new_tag);
                tag[tag_num].tagkey_ = sql.substr(pos, length_tagkey);
                tag[tag_num].tagkey_length_ = length_tagkey;
                j++;
                int flag = 0;
                int length_tagvalue = 0;
                pos = j;
                for (; j < sql.length(); j++) {
                    if (sql[j] == ',') {
                        flag = 1;
                        break;
                    }
                    else if (sql[j] == ' ') {
                        flag = 2;
                        break;
                    }
                    else  length_tagvalue++;
                }
                if (sql[j] != ',' && sql[j] != ' ') {
                    cout << "输入错误请重新输入(error:3)" << endl;
                    ERROR = true;
                    continue;
                }
                tag[tag_num].tagvalue_ = sql.substr(pos, length_tagvalue);
                tag[tag_num].tagvalue_length_ = length_tagvalue;
                tag_num++;
                pos += length_tagvalue;
                if (sql[j] == ',')j++;
                if (j == sql.length()) {
                    getline(cin, sql);
                    while (sql.length() == 0) getline(cin, sql);
                    for (int k = 0; k < sql.length(); k++) {
                        if (sql[k] != ' ')break;
                        pos = k;
                   }
                    j = pos;
                }
                if (flag == 1)continue;
                if (flag == 2)break;
            }
            if (ERROR == true) continue;
            SeriesKey serieskey((uint16_t)measurement.length(), measurement, tag_num, tag);

            serieskey.Init();

            //处理fileds
            vector<string>field_key;
            vector<string>field_value;
            vector<string>value_type;
            while (!ERROR) {
                int length_fieldkey = 0;
                for (; pos < sql.length(); pos++) {
                    if (sql[pos] != ' ')break;
                }
                for (j = pos; j < sql.length(); j++) {
                    if (sql[j] == '=')break;
                    length_fieldkey++;
                }
                if (sql[j] != '=') {
                    cout << "输入错误请重新输入(error:4)" << endl;
                    ERROR = true;
                    continue;
                }
                field_key.push_back(sql.substr(pos, length_fieldkey));
                pos += length_fieldkey + 1;
                int length_fieldvalue = 0;
                int flag = 0;
                for (j = pos; j < sql.length(); j++) {
                    if (sql[j] == ',' || sql[j] == ' ') break;
                    length_fieldvalue++;
                }
                if (sql[j] != ',' && sql[j] != ' ' && j != sql.length()) {
                    cout << "输入错误请重新输入(error:5)" << endl;

                    ERROR = true;
                    continue;
                }
                if (j == sql.length() || sql[j] == ' ') {
                    flag = 2;
                }
                else if (sql[j] == ',') {
                    flag = 1;
                }
                string field_value_tmp = sql.substr(pos, length_fieldvalue);
                pos += length_fieldvalue;
                char value_type_tmp[8];

                if (field_value_tmp[length_fieldvalue - 1] == 'i') {
                    memcpy(value_type_tmp, "int", 8);
                    field_value_tmp = field_value_tmp.substr(0, length_fieldvalue - 1);
                    length_fieldvalue--;
                }
                else if (field_value_tmp[length_fieldvalue - 2] == 'd') {
                    memcpy(value_type_tmp, "double", 8);
                    field_value_tmp = field_value_tmp.substr(0, length_fieldvalue - 1);
                    length_fieldvalue--;
                }
                else if (field_value_tmp == "true" || field_value_tmp == "false") memcpy(value_type_tmp, "bool", 8);
                else memcpy(value_type_tmp, "string", 8);

                field_value.push_back(field_value_tmp);
                value_type.push_back(value_type_tmp);

                if (j == sql.length()&&flag == 1) {
                    getline(cin, sql);
                    while (sql.length() == 0) getline(cin, sql);
                    pos = 0;
                }
                if (flag == 1)continue;
                if (flag == 2)break;
            }

            if (ERROR == true) continue;

            for (; pos < sql.length(); pos++) {
                if (pos != ' ') break;
            }
            int flag = 0;
            long long timestamp;
            if (pos == sql.length()) {
                timestamp = time(nullptr);
                for (int k = 0; k < field_key.size(); k++) {
                    if (!store->InsertData(serieskey, field_key[k], field_value[k], value_type[k], timestamp)) {
                        flag = 1;                    
                        break;
                    }
                }
                if (flag == 0) cout << "完成" << endl;
                else {
                    cout << "请先指定数据库!" << endl;
                    continue;
                }
            }
            else {
                int length_time = 0;
                for (j = pos; j < sql.length(); j++) {
                    if (sql[j] < '0' && sql[j]>'9') {
                        cout << "时间戳指定出错(error:6)" << endl;
                        flag = -1;
                        break;
                    }
                    length_time++;
                }
                if (flag == -1) continue;

                string time = sql.substr(pos, length_time);
                timestamp = atof(time.c_str());
                for (int k = 0; k < field_key.size(); k++) {
                    if (!store->InsertData(serieskey, field_key[k], field_value[k], value_type[k], timestamp)) {
                        flag = 1;
                        break;
                    }
                }
                if (flag == 0) cout << "完成" << endl;
                else {
                    cout << "请先指定数据库!" << endl;
                    continue;
                }
            }
        }
        else if (function == "use") {
            int length_tar = 0;
            int flag = 0;
            int pos_tar=  pos_then;
            for (int i = pos_then; i < sql.length(); i++) {
                if (sql[i] == space && flag == 0) {
                    pos_tar++;
                    continue;
                }
                if (sql[i] == space && flag == 1) break;
                length_tar++;
                flag = 1;
            }

            if (length_tar == 0) {
                cout << "请重新输入正确的语句(缺少use的数据库目标)" << endl;
                continue;
            }
            string tar = sql.substr(pos_tar, length_tar);
            flag = 0;
            if (pos_tar + length_tar < sql.size()) {
                for (int i = pos_tar + length_tar; i < sql.size(); i++) {
                    if (sql[i] != space) {
                        cout << "未能识别" << tar << "之后的字符串, 请重新输入" << endl;
                        flag = 1;
                        break;
                    }
                }
                if (flag == 1) {
                    continue;
                }
            }
            if (!store->SetCurDatabase(tar)) {
                cout << "当前数据库不存在" << endl;
                continue;
            }
            cout << "OK!" << endl;
            continue;
        }
        else if (function == "flush") {
            store->FlushToDisk();
            cout << "完成" << endl;
        }
        else if (function == "exit" || function == "quit") {
            store->FlushToDisk();
            break;
        }
        else {
            cout << "请输入正确的功能" << endl;
            continue;
        }

    }
    delete plan;
    cout << "byebye" << endl;
    return 0;
}

