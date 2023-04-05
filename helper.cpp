#include"helper.h"

void getAllFiles(string path, vector<string>& files) {
	//文件句柄
	long long hFile = 0;
	//文件信息
	struct _finddata_t fileinfo;
	string p;
	int flag = 0;
	if ((hFile = _findfirst(p.assign(path).append("\\*").c_str(), &fileinfo)) != -1) {
		do {
			if (flag < 2) {
				flag++;
				continue;
			}
			files.push_back(p.assign(path).append("\\").append(fileinfo.name));
		} while (_findnext(hFile, &fileinfo) == 0);  //寻找下一个，成功返回0，否则-1
		_findclose(hFile);
	}
}

void getAllFilesName(string path, vector<string>& files) {
	//文件句柄
	long long hFile = 0;
	//文件信息
	struct _finddata_t fileinfo;
	string p;
	int flag = 0;
	if ((hFile = _findfirst(p.assign(path).append("\\*").c_str(), &fileinfo)) != -1) {
		do {
			if (flag < 2) {
				flag++;
				continue;
			}
			files.push_back(fileinfo.name);
		} while (_findnext(hFile, &fileinfo) == 0);  //寻找下一个，成功返回0，否则-1
		_findclose(hFile);
	}
}

//用于删除指定路径下的所有文件与文件夹
void listFiles(string dir) {
	//在目录后面加上"\\*.*"进行第一次搜索
	string newDir = dir + "\\*.*";
	//用于查找的句柄
	intptr_t handle;
	struct _finddata_t fileinfo;
	//第一次查找
	handle = _findfirst(newDir.c_str(), &fileinfo);

	if (handle == -1) {
		cout << "无文件" << endl;
		system("pause");
		return;
	}

	do
	{
		if (fileinfo.attrib & _A_SUBDIR) {//如果为文件夹，加上文件夹路径，再次遍历
			if (strcmp(fileinfo.name, ".") == 0 || strcmp(fileinfo.name, "..") == 0)
				continue;

			// 在目录后面加上"\\"和搜索到的目录名进行下一次搜索
			newDir = dir + "\\" + fileinfo.name;
			listFiles(newDir.c_str());//先遍历删除文件夹下的文件，再删除空的文件夹
			cout << newDir.c_str() << endl;
			if (_rmdir(newDir.c_str()) == 0) {//删除空文件夹
				cout << "delete empty dir success" << endl;
			}
			else {
				cout << "delete empty dir error" << endl;
			}
		}
		else {
			string file_path = dir + "\\" + fileinfo.name;
			cout << file_path.c_str() << endl;
			if (remove(file_path.c_str()) == 0) {//删除文件
				cout << "delete file success" << endl;
			}
			else {
				cout << "delete file error" << endl;
			}
		}
	} while (!_findnext(handle, &fileinfo));

	_findclose(handle);
	return;
}